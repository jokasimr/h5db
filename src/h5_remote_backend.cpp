#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#include <libssh2.h>
#include <libssh2_sftp.h>

#ifndef _WIN32
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#endif

#include "h5_remote_backend.hpp"
#include "h5_functions.hpp"
#include "h5_internal.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/path.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/caching_file_system.hpp"

#include <cstring>
#include <cstdlib>
#include <array>
#include <map>
#include <mutex>
#include <optional>
#include <tuple>
#include <vector>

namespace duckdb {

namespace {

static bool StartsWithCI(const std::string &path, const char *prefix) {
	return StringUtil::StartsWith(StringUtil::Lower(path), prefix);
}

static bool IsSftpPath(const std::string &path) {
	return StartsWithCI(path, "sftp://");
}

static bool TryGetDuckDBFsRemoteExtension(const std::string &path, std::string &required_extension) {
	return !IsSftpPath(path) && FileSystem::IsRemoteFile(path, required_extension);
}

class DuckDBFsRemoteBackend : public H5RemoteBackend {
public:
	explicit DuckDBFsRemoteBackend(ClientContext &context, const std::string &path)
	    : file_system(FileSystem::GetFileSystem(context)), caching_fs(CachingFileSystem::Get(context)), path(path),
	      read_flags(FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO) {
		OpenFileInfo open_info(path);
		cached_handle = caching_fs.OpenFile(QueryContext(context), open_info, read_flags);
		if (!cached_handle) {
			throw IOException("Failed to open remote file '%s'", path);
		}
		file_size = cached_handle->GetFileSize();
	}

	idx_t GetFileSize() const override {
		return file_size;
	}

	void ReadCached(idx_t offset, idx_t size, void *buf) override {
		if (!cached_handle) {
			throw IOException("Failed to read remote data: no readable file handle");
		}
		data_ptr_t buffer = nullptr;
		auto pinned_buffer = cached_handle->Read(buffer, size, offset);
		if (!pinned_buffer.IsValid() || !buffer) {
			throw IOException("Failed to read remote data: invalid cached buffer");
		}
		std::memcpy(buf, buffer, size);
	}

	void ReadDirect(idx_t offset, idx_t size, void *buf) override {
		OpenDirectHandleIfNeeded();
		if (!direct_handle) {
			throw IOException("Failed to read remote data: no readable direct file handle");
		}
		direct_handle->Read(buf, size, offset);
	}

private:
	void OpenDirectHandleIfNeeded() {
		if (direct_handle) {
			return;
		}
		OpenFileInfo open_info(path);
		direct_handle = file_system.OpenFile(open_info, read_flags);
		if (!direct_handle) {
			throw IOException("Failed to open remote file '%s' for direct reads", path);
		}
	}

	FileSystem &file_system;
	CachingFileSystem caching_fs;
	std::string path;
	FileOpenFlags read_flags;
	unique_ptr<CachingFileHandle> cached_handle;
	unique_ptr<FileHandle> direct_handle;
	idx_t file_size = 0;
};

struct H5SftpUrl {
	std::string original_url;
	std::string authority;
	std::string host;
	std::string remote_path;
	std::optional<std::string> username;
	std::optional<int> port;
};

using SecretScopeCacheKey = std::pair<std::string, vector<string>>;

struct H5SftpConfig {
	std::string host;
	int port = 22;
	std::string username;
	std::optional<std::string> password;
	std::optional<std::string> key_path;
	std::optional<std::string> key_passphrase;
	std::optional<std::string> known_hosts_path;
	std::optional<std::string> host_key_fingerprint;
	std::optional<std::string> host_key_algorithms;
	SecretScopeCacheKey secret_scope_cache_key;
	std::string remote_path;
};

struct SftpHostKeyHintCacheKey {
	SecretScopeCacheKey scope_key;
	std::string host;
	int port;
	std::string known_hosts_path;
	std::string host_key_fingerprint;
	std::string host_key_algorithms;

	bool operator<(const SftpHostKeyHintCacheKey &other) const {
		return std::tie(scope_key, host, port, known_hosts_path, host_key_fingerprint, host_key_algorithms) <
		       std::tie(other.scope_key, other.host, other.port, other.known_hosts_path, other.host_key_fingerprint,
		                other.host_key_algorithms);
	}
};

using H5SftpConnectionCacheKey = std::tuple<std::string, int, std::string, std::optional<hash_t>, std::string,
                                            std::optional<hash_t>, std::string, std::string, std::string>;

// Access to this process-global hint cache is currently serialized by hdf5_global_mutex.
// H5SftpConnection is only constructed from the remote HDF5 VFD open path, and file opens happen while that
// mutex is held. If the SFTP backend is ever used outside that path, this cache would need explicit synchronization.
static std::map<SftpHostKeyHintCacheKey, std::string> sftp_host_key_hint_cache;

static SecretScopeCacheKey BuildSecretScopeCacheKey(const BaseSecret &secret) {
	return {secret.GetName(), secret.GetScope()};
}

static int HostKeyTypeToKnownHostMask(int hostkey_type) {
	switch (hostkey_type) {
	case LIBSSH2_HOSTKEY_TYPE_RSA:
		return LIBSSH2_KNOWNHOST_KEY_SSHRSA;
	case LIBSSH2_HOSTKEY_TYPE_DSS:
		return LIBSSH2_KNOWNHOST_KEY_SSHDSS;
	case LIBSSH2_HOSTKEY_TYPE_ECDSA_256:
		return LIBSSH2_KNOWNHOST_KEY_ECDSA_256;
	case LIBSSH2_HOSTKEY_TYPE_ECDSA_384:
		return LIBSSH2_KNOWNHOST_KEY_ECDSA_384;
	case LIBSSH2_HOSTKEY_TYPE_ECDSA_521:
		return LIBSSH2_KNOWNHOST_KEY_ECDSA_521;
	case LIBSSH2_HOSTKEY_TYPE_ED25519:
		return LIBSSH2_KNOWNHOST_KEY_ED25519;
	default:
		return 0;
	}
}

static std::string HostKeyTypeToString(int hostkey_type, const char *method_name) {
	if (method_name && method_name[0] != '\0') {
		return method_name;
	}
	switch (hostkey_type) {
	case LIBSSH2_HOSTKEY_TYPE_RSA:
		return "ssh-rsa";
	case LIBSSH2_HOSTKEY_TYPE_DSS:
		return "ssh-dss";
	case LIBSSH2_HOSTKEY_TYPE_ECDSA_256:
		return "ecdsa-sha2-nistp256";
	case LIBSSH2_HOSTKEY_TYPE_ECDSA_384:
		return "ecdsa-sha2-nistp384";
	case LIBSSH2_HOSTKEY_TYPE_ECDSA_521:
		return "ecdsa-sha2-nistp521";
	case LIBSSH2_HOSTKEY_TYPE_ED25519:
		return "ssh-ed25519";
	default:
		return "unknown";
	}
}

static std::string KnownHostCheckToString(int check) {
	switch (check) {
	case LIBSSH2_KNOWNHOST_CHECK_MATCH:
		return "MATCH";
	case LIBSSH2_KNOWNHOST_CHECK_MISMATCH:
		return "MISMATCH";
	case LIBSSH2_KNOWNHOST_CHECK_NOTFOUND:
		return "NOTFOUND";
	case LIBSSH2_KNOWNHOST_CHECK_FAILURE:
		return "FAILURE";
	default:
		return StringUtil::Format("UNKNOWN(%d)", check);
	}
}

static std::string GetSftpRemotePath(const Path &path) {
	std::string result = path.GetAnchor();
	for (idx_t i = 0; i < path.GetPathSegments().size(); i++) {
		if (i > 0 || result.empty() || result.back() != path.GetSeparator()) {
			result.push_back(path.GetSeparator());
		}
		result += path.GetPathSegments()[i];
	}
	if (result.empty()) {
		return "/";
	}
	auto trailing_separator = path.GetTrailingSeparator();
	if (!trailing_separator.empty() && result.back() != path.GetSeparator()) {
		result += trailing_separator;
	}
	return result;
}

static int ParseSftpPort(const std::string &port_text, const std::string &url) {
	size_t parsed_chars = 0;
	int64_t port = 0;
	try {
		port = std::stoll(port_text, &parsed_chars, 10);
	} catch (...) {
		throw InvalidInputException("Invalid port in sftp URL: %s", url);
	}
	if (parsed_chars != port_text.size() || port < 1 || port > 65535) {
		throw InvalidInputException("Invalid port in sftp URL: %s", url);
	}
	return UnsafeNumericCast<int>(port);
}

static H5SftpUrl ParseSftpUrl(const std::string &url) {
	auto parsed = Path::FromString(url);
	auto scheme = StringUtil::Lower(parsed.GetScheme());
	if (scheme != "sftp://") {
		throw InvalidInputException("Expected sftp:// URL, got '%s'", url);
	}
	if (!parsed.HasAuthority()) {
		throw InvalidInputException("sftp URL must contain a host: %s", url);
	}

	H5SftpUrl result;
	result.original_url = url;
	result.authority = parsed.GetAuthority();
	result.remote_path = GetSftpRemotePath(parsed);

	auto authority = parsed.GetAuthority();
	auto at_pos = authority.rfind('@');
	std::string host_port = authority;
	if (at_pos != std::string::npos) {
		result.username = authority.substr(0, at_pos);
		host_port = authority.substr(at_pos + 1);
	}

	if (host_port.empty()) {
		throw InvalidInputException("sftp URL must contain a host: %s", url);
	}
	if (host_port.front() == '[') {
		auto closing = host_port.find(']');
		if (closing == std::string::npos) {
			throw InvalidInputException("Invalid IPv6 host in sftp URL: %s", url);
		}
		result.host = host_port.substr(1, closing - 1);
		if (closing + 1 < host_port.size()) {
			if (host_port[closing + 1] != ':') {
				throw InvalidInputException("Invalid host/port in sftp URL: %s", url);
			}
			result.port = ParseSftpPort(host_port.substr(closing + 2), url);
		}
	} else {
		auto colon = host_port.rfind(':');
		if (colon != std::string::npos) {
			result.host = host_port.substr(0, colon);
			result.port = ParseSftpPort(host_port.substr(colon + 1), url);
		} else {
			result.host = host_port;
		}
	}
	if (result.host.empty()) {
		throw InvalidInputException("sftp URL must contain a host: %s", url);
	}
	if (result.remote_path.empty() || result.remote_path[0] != '/') {
		throw InvalidInputException("sftp URL must contain an absolute remote path: %s", url);
	}
	return result;
}

static std::string ToHexLower(const unsigned char *data, size_t len) {
	static constexpr const char *HEX = "0123456789abcdef";
	std::string result;
	result.resize(len * 2);
	for (size_t i = 0; i < len; i++) {
		result[i * 2] = HEX[(data[i] >> 4) & 0x0F];
		result[i * 2 + 1] = HEX[data[i] & 0x0F];
	}
	return result;
}

static H5SftpConfig ResolveSftpConfig(ClientContext &context, const H5SftpUrl &url) {
	H5SftpConfig config;
	config.host = url.host;
	config.remote_path = url.remote_path;

	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto secret_match = secret_manager.LookupSecret(transaction, url.original_url, "sftp");
	if (!secret_match.HasMatch()) {
		throw InvalidConfigurationException("No sftp secret found for '%s'", url.original_url);
	}
	const auto &secret = dynamic_cast<const KeyValueSecret &>(*secret_match.secret_entry->secret);
	config.secret_scope_cache_key = BuildSecretScopeCacheKey(secret);

	Value value;
	if (!secret.TryGetValue("username", value)) {
		throw InvalidConfigurationException("sftp secret for '%s' is missing USERNAME", url.original_url);
	}
	config.username = value.GetValue<string>();

	if (secret.TryGetValue("password", value)) {
		config.password = value.GetValue<string>();
	}
	if (secret.TryGetValue("key_path", value)) {
		config.key_path = value.GetValue<string>();
	}
	if (secret.TryGetValue("key_passphrase", value)) {
		config.key_passphrase = value.GetValue<string>();
	}
	if (secret.TryGetValue("port", value)) {
		config.port = value.GetValue<int32_t>();
	}
	if (secret.TryGetValue("known_hosts_path", value)) {
		config.known_hosts_path = value.GetValue<string>();
	}
	if (secret.TryGetValue("host_key_fingerprint", value)) {
		config.host_key_fingerprint = StringUtil::Lower(value.GetValue<string>());
	}
	if (secret.TryGetValue("host_key_algorithms", value)) {
		config.host_key_algorithms = value.GetValue<string>();
	}

	if (url.username && *url.username != config.username) {
		throw InvalidConfigurationException("sftp URL username '%s' does not match secret username '%s'", *url.username,
		                                    config.username);
	}
	if (url.port && *url.port != config.port) {
		throw InvalidConfigurationException("sftp URL port '%d' does not match secret port '%d'", *url.port,
		                                    config.port);
	}
	if (config.password.has_value() == config.key_path.has_value()) {
		throw InvalidConfigurationException("sftp secret for '%s' must contain exactly one of PASSWORD or KEY_PATH",
		                                    url.original_url);
	}
	if (!config.known_hosts_path && !config.host_key_fingerprint) {
		throw InvalidConfigurationException(
		    "sftp secret for '%s' must contain KNOWN_HOSTS_PATH or HOST_KEY_FINGERPRINT", url.original_url);
	}
	return config;
}

static std::optional<hash_t> HashOptionalSecret(const std::optional<std::string> &value) {
	if (!value) {
		return std::nullopt;
	}
	return Hash(value->c_str(), value->size());
}

static H5SftpConnectionCacheKey BuildSftpConnectionCacheKey(const H5SftpConfig &config) {
	return {config.host,
	        config.port,
	        config.username,
	        HashOptionalSecret(config.password),
	        config.key_path.value_or(""),
	        HashOptionalSecret(config.key_passphrase),
	        config.known_hosts_path.value_or(""),
	        config.host_key_fingerprint.value_or(""),
	        config.host_key_algorithms.value_or("")};
}

class ScopedSocket {
public:
	explicit ScopedSocket(libssh2_socket_t fd_p) : fd(fd_p) {
	}
	~ScopedSocket() {
		if (fd != LIBSSH2_INVALID_SOCKET) {
			LIBSSH2_SOCKET_CLOSE(fd);
		}
	}
	libssh2_socket_t Get() const {
		return fd;
	}

private:
	libssh2_socket_t fd;
};

class H5SftpConnection {
public:
	H5SftpConnection(ClientContext &context_p, const H5SftpConfig &config_p) : config(config_p), context(&context_p) {
		InitializeSocketApi();
		InitializeLibssh2();
		OpenVerifiedSession();
		Authenticate();
		OpenSftpSession();
	}

	~H5SftpConnection() {
		ResetConnectionState();
	}

	LIBSSH2_SESSION *GetSession() const {
		return session;
	}

	LIBSSH2_SFTP *GetSftpSession() const {
		return sftp_session;
	}

	ClientContext *GetContext() const {
		return context;
	}

	void ThrowIfDead() const {
		if (IsDead()) {
			throw IOException("SFTP connection is no longer usable");
		}
	}

	void WaitForSessionIO() {
		ThrowIfDead();
		D_ASSERT(session);
		auto directions = libssh2_session_block_directions(session);
		auto wait_read = (directions & LIBSSH2_SESSION_BLOCK_INBOUND) != 0;
		auto wait_write = (directions & LIBSSH2_SESSION_BLOCK_OUTBOUND) != 0;
		if (!wait_read && !wait_write) {
			wait_read = true;
			wait_write = true;
		}
		WaitForSocketEvents(wait_read, wait_write);
	}

	std::string LastSessionError() const {
		if (!session) {
			return "unknown libssh2 error";
		}
		char *message = nullptr;
		int message_len = 0;
		libssh2_session_last_error(session, &message, &message_len, 0);
		if (message && message_len > 0) {
			return std::string(message, UnsafeNumericCast<size_t>(message_len));
		}
		return "unknown libssh2 error";
	}

	void MarkDead() {
		dead = true;
	}

	bool IsDead() const {
		return dead;
	}

private:
	struct HostVerificationResult {
		bool success = false;
		bool retryable = false;
		std::string negotiated_algorithm;
		std::string error_message;
	};

	static void InitializeLibssh2() {
		static std::once_flag init_once;
		static int init_result = 0;
		std::call_once(init_once, []() { init_result = libssh2_init(0); });
		if (init_result != 0) {
			throw IOException("Failed to initialize libssh2");
		}
	}

	static void InitializeSocketApi() {
#ifdef _WIN32
		struct WinsockState {
			WinsockState() {
				WSADATA wsadata;
				auto rc = WSAStartup(MAKEWORD(2, 2), &wsadata);
				if (rc != 0) {
					throw IOException("WSAStartup failed: %d", rc);
				}
				if (LOBYTE(wsadata.wVersion) != 2 || HIBYTE(wsadata.wVersion) != 2) {
					WSACleanup();
					throw IOException("WSAStartup did not negotiate Winsock 2.2");
				}
			}

			~WinsockState() {
				WSACleanup();
			}
		};

		static WinsockState winsock_state;
#endif
	}

	static const std::vector<std::string> &GetSupportedHostKeyAlgorithms() {
		static std::once_flag supported_once;
		static std::vector<std::string> supported_algorithms;

		std::call_once(supported_once, []() {
			auto *supported_session = libssh2_session_init();
			if (!supported_session) {
				return;
			}
			const char **algs = nullptr;
			auto rc = libssh2_session_supported_algs(supported_session, LIBSSH2_METHOD_HOSTKEY, &algs);
			if (rc > 0 && algs) {
				supported_algorithms.reserve(UnsafeNumericCast<size_t>(rc));
				for (int i = 0; i < rc; i++) {
					supported_algorithms.emplace_back(algs[i]);
				}
				libssh2_free(supported_session, const_cast<char **>(algs));
			}
			libssh2_session_free(supported_session);
		});

		return supported_algorithms;
	}

	SftpHostKeyHintCacheKey GetHostKeyHintCacheKey() const {
		return {config.secret_scope_cache_key,
		        config.host,
		        config.port,
		        config.known_hosts_path.value_or(""),
		        config.host_key_fingerprint.value_or(""),
		        config.host_key_algorithms.value_or("")};
	}

	std::vector<std::string> BuildHostKeyAlgorithmAttempts() const {
		std::vector<std::string> attempts;
		case_insensitive_set_t seen;

		auto add_attempt = [&](const std::string &attempt) {
			if (seen.insert(attempt).second) {
				attempts.push_back(attempt);
			}
		};

		if (config.host_key_algorithms) {
			add_attempt(*config.host_key_algorithms);
		}

		auto lookup = sftp_host_key_hint_cache.find(GetHostKeyHintCacheKey());
		if (lookup != sftp_host_key_hint_cache.end()) {
			add_attempt(lookup->second);
		}

		add_attempt("");
		for (const auto &algorithm : GetSupportedHostKeyAlgorithms()) {
			add_attempt(algorithm);
		}
		return attempts;
	}

	void ResetConnectionState() {
		auto abortive = context && context->interrupted.load(std::memory_order_relaxed);
		if (session) {
			libssh2_session_set_blocking(session, 1);
		}
		if (abortive) {
			socket_fd.reset();
		}
		if (sftp_session) {
			libssh2_sftp_shutdown(sftp_session);
			sftp_session = nullptr;
		}
		if (session) {
			if (!abortive) {
				libssh2_session_disconnect(session, "Normal Shutdown");
			}
			libssh2_session_free(session);
			session = nullptr;
		}
		socket_fd.reset();
	}

	void ThrowIfInterrupted() const {
		if (context && context->interrupted.load(std::memory_order_relaxed)) {
			throw InterruptException();
		}
	}

	static bool SocketConnectInProgress(int error_code) {
#ifdef _WIN32
		return error_code == WSAEWOULDBLOCK || error_code == WSAEINPROGRESS || error_code == WSAEALREADY;
#else
		return error_code == EINPROGRESS || error_code == EWOULDBLOCK;
#endif
	}

	static bool SocketWaitInterrupted(int error_code) {
#ifdef _WIN32
		return error_code == WSAEINTR;
#else
		return error_code == EINTR;
#endif
	}

	static int GetSocketLastError() {
#ifdef _WIN32
		return WSAGetLastError();
#else
		return errno;
#endif
	}

	static std::string SocketErrorMessage(int error_code) {
#ifdef _WIN32
		return StringUtil::Format("WSA error %d", error_code);
#else
		return std::strerror(error_code);
#endif
	}

	static bool TrySetSocketNonBlocking(libssh2_socket_t fd) {
#ifdef _WIN32
		u_long non_blocking = 1;
		return ioctlsocket(fd, FIONBIO, &non_blocking) == 0;
#else
		auto flags = fcntl(fd, F_GETFL, 0);
		if (flags < 0) {
			return false;
		}
		return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
#endif
	}

	void WaitForSocketEvents(bool wait_read, bool wait_write) {
		D_ASSERT(socket_fd);

		while (true) {
			ThrowIfInterrupted();
#ifdef _WIN32
			fd_set readfds;
			fd_set writefds;
			fd_set exceptfds;
			FD_ZERO(&readfds);
			FD_ZERO(&writefds);
			FD_ZERO(&exceptfds);
			if (wait_read) {
				FD_SET(socket_fd->Get(), &readfds);
			}
			if (wait_write) {
				FD_SET(socket_fd->Get(), &writefds);
			}
			FD_SET(socket_fd->Get(), &exceptfds);

			timeval timeout;
			timeout.tv_sec = 0;
			timeout.tv_usec = IO_WAIT_SLICE_MS * 1000;

			auto rc = select(0, wait_read ? &readfds : nullptr, wait_write ? &writefds : nullptr, &exceptfds, &timeout);
			if (rc > 0) {
				return;
			}
			if (rc == 0) {
				continue;
			}
			auto error_code = GetSocketLastError();
			if (SocketWaitInterrupted(error_code)) {
				continue;
			}
			throw IOException("Waiting on SFTP socket failed: %s", SocketErrorMessage(error_code));
#else
			short events = 0;
			if (wait_read) {
				events |= POLLIN;
			}
			if (wait_write) {
				events |= POLLOUT;
			}

			struct pollfd pfd;
			pfd.fd = socket_fd->Get();
			pfd.events = events;
			pfd.revents = 0;

			auto rc = poll(&pfd, 1, IO_WAIT_SLICE_MS);
			if (rc > 0) {
				return;
			}
			if (rc == 0) {
				continue;
			}
			auto error_code = GetSocketLastError();
			if (SocketWaitInterrupted(error_code)) {
				continue;
			}
			throw IOException("Waiting on SFTP socket failed: %s", SocketErrorMessage(error_code));
#endif
		}
	}

	void ConnectSocket() {
		ThrowIfInterrupted();
		struct addrinfo hints;
		std::memset(&hints, 0, sizeof(hints));
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_family = AF_UNSPEC;

		struct addrinfo *result = nullptr;
		auto port_string = std::to_string(config.port);
		auto rc = getaddrinfo(config.host.c_str(), port_string.c_str(), &hints, &result);
		if (rc != 0) {
#ifdef _WIN32
			throw IOException("Failed to resolve SFTP host '%s': %s", config.host, gai_strerrorA(rc));
#else
			throw IOException("Failed to resolve SFTP host '%s': %s", config.host, gai_strerror(rc));
#endif
		}

		std::unique_ptr<struct addrinfo, decltype(&freeaddrinfo)> addrinfo_guard(result, freeaddrinfo);
		libssh2_socket_t fd = LIBSSH2_INVALID_SOCKET;
		for (auto *rp = result; rp; rp = rp->ai_next) {
			ThrowIfInterrupted();
			fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
			if (fd == LIBSSH2_INVALID_SOCKET) {
				continue;
			}
			if (!TrySetSocketNonBlocking(fd)) {
				LIBSSH2_SOCKET_CLOSE(fd);
				fd = LIBSSH2_INVALID_SOCKET;
				continue;
			}
			if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
				socket_fd = make_uniq<ScopedSocket>(fd);
				return;
			}

			auto connect_error = GetSocketLastError();
			if (SocketConnectInProgress(connect_error)) {
				socket_fd = make_uniq<ScopedSocket>(fd);
				WaitForSocketEvents(false, true);

				int socket_error = 0;
#ifdef _WIN32
				int error_len = sizeof(socket_error);
				if (getsockopt(fd, SOL_SOCKET, SO_ERROR, reinterpret_cast<char *>(&socket_error), &error_len) == 0 &&
				    socket_error == 0) {
					return;
				}
#else
				socklen_t error_len = sizeof(socket_error);
				if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &error_len) == 0 && socket_error == 0) {
					return;
				}
#endif

				socket_fd.reset();
				fd = LIBSSH2_INVALID_SOCKET;
				continue;
			}
			LIBSSH2_SOCKET_CLOSE(fd);
			fd = LIBSSH2_INVALID_SOCKET;
		}
		throw IOException("Failed to connect to SFTP host '%s:%d'", config.host, config.port);
	}

	void OpenSession(const char *host_key_preferences) {
		session = libssh2_session_init();
		if (!session) {
			throw IOException("Failed to create libssh2 session");
		}
		if (host_key_preferences && host_key_preferences[0] != '\0') {
			auto rc = libssh2_session_method_pref(session, LIBSSH2_METHOD_HOSTKEY, host_key_preferences);
			if (rc < 0) {
				throw IOException("Failed to set SSH host key algorithm preference '%s': %s", host_key_preferences,
				                  LastSessionError());
			}
		}
		libssh2_session_set_blocking(session, 0);
		while (true) {
			auto rc = libssh2_session_handshake(session, socket_fd->Get());
			if (rc == 0) {
				return;
			}
			if (rc != LIBSSH2_ERROR_EAGAIN) {
				throw IOException("Failed SSH handshake: %s", LastSessionError());
			}
			WaitForSessionIO();
		}
	}

	HostVerificationResult VerifyHost() {
		HostVerificationResult result;
		size_t hostkey_len = 0;
		int hostkey_type = 0;
		const char *hostkey = libssh2_session_hostkey(session, &hostkey_len, &hostkey_type);
		if (!hostkey || hostkey_len == 0) {
			result.error_message = "Failed to retrieve SSH host key";
			return result;
		}

		const auto *negotiated_method = libssh2_session_methods(session, LIBSSH2_METHOD_HOSTKEY);
		result.negotiated_algorithm = HostKeyTypeToString(hostkey_type, negotiated_method);

		if (config.known_hosts_path) {
			auto *known_hosts = libssh2_knownhost_init(session);
			if (!known_hosts) {
				result.error_message = "Failed to initialize SSH known_hosts";
				return result;
			}
			if (libssh2_knownhost_readfile(known_hosts, config.known_hosts_path->c_str(),
			                               LIBSSH2_KNOWNHOST_FILE_OPENSSH) < 0) {
				libssh2_knownhost_free(known_hosts);
				result.error_message =
				    StringUtil::Format("Failed to read known_hosts file '%s'", *config.known_hosts_path);
				return result;
			}

			struct libssh2_knownhost *host = nullptr;
			auto check = libssh2_knownhost_checkp(known_hosts, config.host.c_str(), config.port, hostkey, hostkey_len,
			                                      LIBSSH2_KNOWNHOST_TYPE_PLAIN | LIBSSH2_KNOWNHOST_KEYENC_RAW |
			                                          HostKeyTypeToKnownHostMask(hostkey_type),
			                                      &host);
			libssh2_knownhost_free(known_hosts);
			if (check != LIBSSH2_KNOWNHOST_CHECK_MATCH) {
				result.retryable =
				    check == LIBSSH2_KNOWNHOST_CHECK_MISMATCH || check == LIBSSH2_KNOWNHOST_CHECK_NOTFOUND;
				result.error_message = StringUtil::Format(
				    "SSH host key verification failed for '%s:%d' (algorithm=%s, known_hosts=%s)", config.host,
				    config.port, result.negotiated_algorithm, KnownHostCheckToString(check));
				return result;
			}
		}

		if (config.host_key_fingerprint) {
			auto *fingerprint =
			    reinterpret_cast<const unsigned char *>(libssh2_hostkey_hash(session, LIBSSH2_HOSTKEY_HASH_SHA1));
			if (!fingerprint) {
				result.error_message = "Failed to compute SSH host key fingerprint";
				return result;
			}
			auto actual = ToHexLower(fingerprint, 20);
			if (actual != *config.host_key_fingerprint) {
				result.retryable = true;
				result.error_message =
				    StringUtil::Format("SSH host key fingerprint mismatch for '%s:%d' (algorithm=%s)", config.host,
				                       config.port, result.negotiated_algorithm);
				return result;
			}
		}

		result.success = true;
		return result;
	}

	void OpenVerifiedSession() {
		std::string last_error;
		std::string last_verification_error;

		for (const auto &attempt : BuildHostKeyAlgorithmAttempts()) {
			ResetConnectionState();
			ConnectSocket();
			try {
				OpenSession(attempt.empty() ? nullptr : attempt.c_str());
			} catch (const IOException &ex) {
				if (attempt.empty() && last_verification_error.empty()) {
					throw;
				}
				if (last_verification_error.empty()) {
					last_error = ex.what();
				}
				continue;
			}

			auto verification = VerifyHost();
			if (verification.success) {
				sftp_host_key_hint_cache[GetHostKeyHintCacheKey()] = verification.negotiated_algorithm;
				return;
			}
			if (!verification.retryable) {
				ResetConnectionState();
				throw IOException("%s", verification.error_message);
			}
			last_verification_error = verification.error_message;
			last_error = verification.error_message;
		}

		ResetConnectionState();
		if (!last_verification_error.empty()) {
			throw IOException("%s", last_verification_error);
		}
		if (!last_error.empty()) {
			throw IOException("%s", last_error);
		}
		throw IOException("SSH host key verification failed for '%s:%d'", config.host, config.port);
	}

	void Authenticate() {
		if (config.password) {
			while (true) {
				auto rc = libssh2_userauth_password(session, config.username.c_str(), config.password->c_str());
				if (rc == 0) {
					return;
				}
				if (rc != LIBSSH2_ERROR_EAGAIN) {
					throw IOException("SSH password authentication failed for '%s': %s", config.username,
					                  LastSessionError());
				}
				WaitForSessionIO();
			}
		}
		auto passphrase = config.key_passphrase ? config.key_passphrase->c_str() : nullptr;
		D_ASSERT(config.key_path.has_value());
		const auto &key_path = config.key_path.value();
		while (true) {
			auto rc = libssh2_userauth_publickey_fromfile(session, config.username.c_str(), nullptr, key_path.c_str(),
			                                              passphrase);
			if (rc == 0) {
				return;
			}
			if (rc != LIBSSH2_ERROR_EAGAIN) {
				throw IOException("SSH public key authentication failed for '%s': %s", config.username,
				                  LastSessionError());
			}
			WaitForSessionIO();
		}
	}

	void OpenSftpSession() {
		while (true) {
			sftp_session = libssh2_sftp_init(session);
			if (sftp_session) {
				return;
			}
			if (libssh2_session_last_errno(session) != LIBSSH2_ERROR_EAGAIN) {
				throw IOException("Failed to initialize SFTP session: %s", LastSessionError());
			}
			WaitForSessionIO();
		}
	}

	H5SftpConfig config;
	ClientContext *context = nullptr;
	unique_ptr<ScopedSocket> socket_fd;
	LIBSSH2_SESSION *session = nullptr;
	LIBSSH2_SFTP *sftp_session = nullptr;
	// This connection is only touched from the HDF5 VFD path while hdf5_global_mutex is held.
	bool dead = false;
	static constexpr int IO_WAIT_SLICE_MS = 100;
};

class H5SftpConnectionCacheState : public ClientContextState {
public:
	shared_ptr<H5SftpConnection> GetOrCreate(ClientContext &context, const H5SftpConfig &config) {
		auto key = BuildSftpConnectionCacheKey(config);
		auto lookup = connections.find(key);
		if (lookup != connections.end()) {
			if (lookup->second && !lookup->second->IsDead()) {
				return lookup->second;
			}
			connections.erase(lookup);
		}

		auto created = make_shared_ptr<H5SftpConnection>(context, config);
		connections[key] = created;
		return created;
	}

	void QueryEnd() override {
		connections.clear();
	}

private:
	// Access to this query-local cache happens through the HDF5 VFD path while hdf5_global_mutex is held.
	// QueryEnd runs after DuckDB has finished the active query, so no extra synchronization is needed here.
	std::map<H5SftpConnectionCacheKey, shared_ptr<H5SftpConnection>> connections;
};

static shared_ptr<H5SftpConnection> GetOrCreateCachedSftpConnection(ClientContext &context,
                                                                    const H5SftpConfig &config) {
	auto cache_state = context.registered_state->GetOrCreate<H5SftpConnectionCacheState>("h5db_sftp_connection_cache");
	return cache_state->GetOrCreate(context, config);
}

static bool IsCrawlPattern(const std::string &pattern) {
	return pattern == "**";
}

static bool HasMultipleCrawlPatterns(const vector<string> &path_segments) {
	idx_t crawl_count = 0;
	for (const auto &segment : path_segments) {
		if (IsCrawlPattern(segment)) {
			crawl_count++;
		}
	}
	return crawl_count > 1;
}

static std::string JoinSftpPath(const std::string &parent, const std::string &name) {
	if (parent == "/") {
		return "/" + name;
	}
	return parent + "/" + name;
}

struct H5SftpRemotePattern {
	vector<string> components;
	bool has_glob = false;
};

static H5SftpRemotePattern ParseSftpRemotePattern(const std::string &remote_path) {
	auto parsed_path = Path::FromString(remote_path);
	H5SftpRemotePattern result;
	result.components.reserve(parsed_path.GetPathSegments().size() + (parsed_path.HasTrailingSeparator() ? 1 : 0));
	for (const auto &segment : parsed_path.GetPathSegments()) {
		result.has_glob = result.has_glob || FileSystem::HasGlob(segment);
		result.components.push_back(segment);
	}
	if (parsed_path.HasTrailingSeparator()) {
		result.components.emplace_back();
	}
	return result;
}

struct H5SftpListEntry {
	std::string name;
	bool is_directory = false;
	bool is_symbolic_link = false;
};

enum class H5SftpPathKind : uint8_t { UNKNOWN, OTHER, DIRECTORY, SYMBOLIC_LINK };

class H5SftpGlobExpander {
public:
	H5SftpGlobExpander(shared_ptr<H5SftpConnection> connection_p, std::string authority_p,
	                   H5SftpRemotePattern pattern_p)
	    : connection(std::move(connection_p)), authority(std::move(authority_p)),
	      remote_components(std::move(pattern_p.components)) {
		if (HasMultipleCrawlPatterns(remote_components)) {
			throw IOException("Cannot use multiple '**' in one path");
		}
	}

	vector<std::string> Expand() {
		vector<std::string> matched_paths;
		ExpandFrom("/", 0, matched_paths);

		vector<std::string> result;
		result.reserve(matched_paths.size());
		for (auto &matched_path : matched_paths) {
			result.push_back("sftp://" + authority + matched_path);
		}
		std::sort(result.begin(), result.end());
		return result;
	}

private:
	static H5SftpPathKind PathKindFromAttrs(const LIBSSH2_SFTP_ATTRIBUTES &attrs) {
		if (!(attrs.flags & LIBSSH2_SFTP_ATTR_PERMISSIONS)) {
			return H5SftpPathKind::UNKNOWN;
		}
		if (LIBSSH2_SFTP_S_ISLNK(attrs.permissions)) {
			return H5SftpPathKind::SYMBOLIC_LINK;
		}
		if (LIBSSH2_SFTP_S_ISDIR(attrs.permissions)) {
			return H5SftpPathKind::DIRECTORY;
		}
		return H5SftpPathKind::OTHER;
	}

	H5SftpPathKind TryGetPathKind(const std::string &path, int stat_type) const {
		LIBSSH2_SFTP_ATTRIBUTES attrs {};
		if (!TryStatPath(path, stat_type, attrs)) {
			return H5SftpPathKind::UNKNOWN;
		}
		return PathKindFromAttrs(attrs);
	}

	LIBSSH2_SFTP_HANDLE *OpenDirectoryHandle(const std::string &path, bool allow_probe_failure = false) const {
		connection->ThrowIfDead();
		while (true) {
			auto *dir_handle = libssh2_sftp_opendir(connection->GetSftpSession(), path.c_str());
			if (dir_handle) {
				return dir_handle;
			}
			if (libssh2_session_last_errno(connection->GetSession()) == LIBSSH2_ERROR_EAGAIN) {
				connection->WaitForSessionIO();
				continue;
			}
			auto sftp_error = libssh2_sftp_last_error(connection->GetSftpSession());
			if (allow_probe_failure &&
			    (sftp_error == LIBSSH2_FX_NO_SUCH_FILE || sftp_error == LIBSSH2_FX_NO_SUCH_PATH ||
			     sftp_error == LIBSSH2_FX_FAILURE)) {
				// Servers commonly report "not a directory" from OPENDIR as a generic
				// failure. This probe only runs after STAT confirmed that the path exists
				// but omitted type bits, so treat generic open failure as "not a
				// traversable directory" rather than as a hard error.
				return nullptr;
			}
			throw IOException("Failed to open SFTP directory '%s': %s", path, connection->LastSessionError());
		}
	}

	bool CanOpenDirectory(const std::string &path) const {
		auto *dir_handle = OpenDirectoryHandle(path, true);
		if (!dir_handle) {
			return false;
		}
		CloseDirectoryHandle(dir_handle);
		return true;
	}

	bool TryStatPath(const std::string &path, int stat_type, LIBSSH2_SFTP_ATTRIBUTES &attrs) const {
		connection->ThrowIfDead();
		while (true) {
			auto rc = libssh2_sftp_stat_ex(connection->GetSftpSession(), path.c_str(),
			                               UnsafeNumericCast<unsigned int>(path.size()), stat_type, &attrs);
			if (rc == 0) {
				return true;
			}
			if (rc == LIBSSH2_ERROR_EAGAIN) {
				connection->WaitForSessionIO();
				continue;
			}
			auto sftp_error = libssh2_sftp_last_error(connection->GetSftpSession());
			if (sftp_error == LIBSSH2_FX_NO_SUCH_FILE || sftp_error == LIBSSH2_FX_NO_SUCH_PATH) {
				return false;
			}
			throw IOException("Failed to stat SFTP path '%s': %s", path, connection->LastSessionError());
		}
	}

	H5SftpPathKind TryGetResolvedPathKind(const std::string &path) const {
		LIBSSH2_SFTP_ATTRIBUTES attrs {};
		if (!TryStatPath(path, LIBSSH2_SFTP_STAT, attrs)) {
			return H5SftpPathKind::UNKNOWN;
		}
		auto path_kind = PathKindFromAttrs(attrs);
		if (path_kind != H5SftpPathKind::UNKNOWN) {
			return path_kind;
		}
		return CanOpenDirectory(path) ? H5SftpPathKind::DIRECTORY : H5SftpPathKind::OTHER;
	}

	std::optional<H5SftpListEntry> ClassifyListedPath(const std::string &directory_path, std::string name,
	                                                  const LIBSSH2_SFTP_ATTRIBUTES &attrs) const {
		auto entry_path = JoinSftpPath(directory_path, name);
		auto raw_kind = PathKindFromAttrs(attrs);

		// Some servers omit permission bits from READDIR entries. Fall back to LSTAT to
		// identify the entry itself, then STAT if needed to determine whether a symlink
		// resolves to a directory. This matches DuckDB's local globbing behavior:
		// ordinary glob components can match/traverse symlinks, but recursive '**'
		// expansion still skips symlink entries explicitly.
		if (raw_kind == H5SftpPathKind::UNKNOWN) {
			raw_kind = TryGetPathKind(entry_path, LIBSSH2_SFTP_LSTAT);
		}
		if (raw_kind == H5SftpPathKind::UNKNOWN) {
			return std::nullopt;
		}

		H5SftpListEntry entry;
		entry.name = std::move(name);
		entry.is_directory = raw_kind == H5SftpPathKind::DIRECTORY;
		entry.is_symbolic_link = raw_kind == H5SftpPathKind::SYMBOLIC_LINK;
		if (!entry.is_symbolic_link) {
			return entry;
		}
		auto resolved_kind = TryGetResolvedPathKind(entry_path);
		if (resolved_kind == H5SftpPathKind::UNKNOWN) {
			return std::nullopt;
		}
		entry.is_directory = resolved_kind == H5SftpPathKind::DIRECTORY;
		return entry;
	}

	void CloseDirectoryHandle(LIBSSH2_SFTP_HANDLE *dir_handle) const {
		if (!dir_handle) {
			return;
		}
		while (true) {
			auto rc = libssh2_sftp_closedir(dir_handle);
			if (rc == 0) {
				return;
			}
			if (rc != LIBSSH2_ERROR_EAGAIN) {
				connection->MarkDead();
				return;
			}
			connection->WaitForSessionIO();
		}
	}

	vector<H5SftpListEntry> ListDirectory(const std::string &path) const {
		LIBSSH2_SFTP_HANDLE *dir_handle = OpenDirectoryHandle(path);

		vector<H5SftpListEntry> result;
		std::array<char, 4096> name_buffer {};
		try {
			while (true) {
				LIBSSH2_SFTP_ATTRIBUTES attrs {};
				auto rc =
				    libssh2_sftp_readdir(dir_handle, name_buffer.data(), name_buffer.size(), &attrs);
				if (rc == LIBSSH2_ERROR_EAGAIN) {
					connection->WaitForSessionIO();
					continue;
				}
				if (rc < 0) {
					throw IOException("Failed to list SFTP directory '%s': %s", path, connection->LastSessionError());
				}
				if (rc == 0) {
					break;
				}

				std::string name(name_buffer.data(), UnsafeNumericCast<size_t>(rc));
				if (name == "." || name == "..") {
					continue;
				}

				auto entry = ClassifyListedPath(path, std::move(name), attrs);
				if (!entry) {
					continue;
				}
				result.push_back(std::move(*entry));
			}
		} catch (...) {
			CloseDirectoryHandle(dir_handle);
			throw;
		}
		CloseDirectoryHandle(dir_handle);
		return result;
	}

	void ExpandFrom(const std::string &current_path, idx_t split_index, vector<std::string> &result) const {
		if (split_index >= remote_components.size()) {
			return;
		}

		const auto &component = remote_components[split_index];
		bool is_last_component = split_index + 1 == remote_components.size();

		if (!FileSystem::HasGlob(component)) {
			auto next_path = JoinSftpPath(current_path, component);
			auto path_kind = TryGetResolvedPathKind(next_path);
			if (path_kind == H5SftpPathKind::UNKNOWN) {
				return;
			}
			if (is_last_component) {
				result.push_back(std::move(next_path));
			} else if (path_kind == H5SftpPathKind::DIRECTORY) {
				ExpandFrom(next_path, split_index + 1, result);
			}
			return;
		}

		if (IsCrawlPattern(component)) {
			if (is_last_component) {
				CrawlFiles(current_path, result);
				return;
			}
			ExpandFrom(current_path, split_index + 1, result);
			for (const auto &entry : ListDirectory(current_path)) {
				if (!entry.is_directory || entry.is_symbolic_link) {
					continue;
				}
				ExpandFrom(JoinSftpPath(current_path, entry.name), split_index, result);
			}
			return;
		}

		for (const auto &entry : ListDirectory(current_path)) {
			if (!Glob(entry.name.c_str(), entry.name.size(), component.c_str(), component.size())) {
				continue;
			}
			auto next_path = JoinSftpPath(current_path, entry.name);
			if (is_last_component) {
				if (!entry.is_directory) {
					result.push_back(std::move(next_path));
				}
			} else if (entry.is_directory) {
				ExpandFrom(next_path, split_index + 1, result);
			}
		}
	}

	void CrawlFiles(const std::string &directory_path, vector<std::string> &result) const {
		for (const auto &entry : ListDirectory(directory_path)) {
			if (entry.is_symbolic_link) {
				continue;
			}
			auto entry_path = JoinSftpPath(directory_path, entry.name);
			if (entry.is_directory) {
				CrawlFiles(entry_path, result);
			} else {
				result.push_back(std::move(entry_path));
			}
		}
	}

	shared_ptr<H5SftpConnection> connection;
	const std::string authority;
	const vector<string> remote_components;
};

class H5SftpFileEngine {
public:
	explicit H5SftpFileEngine(shared_ptr<H5SftpConnection> connection_p, const H5SftpConfig &config_p)
	    : connection(std::move(connection_p)), context(connection->GetContext()), remote_path(config_p.remote_path),
	      profiling_enabled(std::getenv("H5DB_SFTP_PROFILE") != nullptr) {
		for (idx_t i = 0; i < sftp_handles.size(); i++) {
			sftp_handles[i].index = i;
		}
		try {
			OpenReadHandle(0);
			StatFile();
		} catch (...) {
			MarkConnectionDeadAfterFileEngineFailure();
			throw;
		}
	}

	~H5SftpFileEngine() {
		if (profiling_enabled) {
			stats.Print(remote_path);
		}
		for (auto &handle_slot : sftp_handles) {
			CloseReadHandle(handle_slot);
		}
	}

	idx_t GetFileSize() const {
		return file_size;
	}

	timestamp_t GetLastModifiedTime() const {
		return last_modified;
	}

	void Read(idx_t offset, idx_t size, void *buf) {
		try {
			ReadInternal(offset, size, buf);
		} catch (const InterruptException &) {
			throw;
		} catch (...) {
			MarkConnectionDeadAfterFileEngineFailure();
			throw;
		}
	}

private:
	static constexpr idx_t SFTP_HANDLE_POOL_SIZE = 4;
	static constexpr idx_t SFTP_HANDLE_LOCALITY_THRESHOLD = 2 * 1024 * 1024;

	struct SftpReadHandle {
		LIBSSH2_SFTP_HANDLE *handle = nullptr;
		std::optional<idx_t> current_offset;
		idx_t index = 0;

		bool IsOpen() const {
			return handle != nullptr;
		}
	};

	struct SftpReadStats {
		idx_t backend_read_calls = 0;
		idx_t backend_bytes_requested = 0;
		idx_t sequential_read_calls = 0;
		idx_t backward_seek_calls = 0;
		idx_t forward_seek_calls = 0;
		idx_t open_handle_count = 0;
		idx_t sftp_read_calls = 0;
		idx_t sftp_bytes_read = 0;
		idx_t largest_forward_gap = 0;
		idx_t largest_backward_gap = 0;
		std::optional<idx_t> next_expected_offset;
		std::array<idx_t, SFTP_HANDLE_POOL_SIZE> handle_use_counts {};
		std::array<idx_t, SFTP_HANDLE_POOL_SIZE> handle_sftp_read_counts {};
		std::map<idx_t, idx_t> requested_size_histogram;
		std::map<idx_t, idx_t> returned_size_histogram;

		void RecordHandleOpened() {
			open_handle_count++;
		}

		void RecordRead(idx_t offset, idx_t size, idx_t handle_index) {
			backend_read_calls++;
			backend_bytes_requested += size;
			requested_size_histogram[size]++;
			handle_use_counts[handle_index]++;
			if (next_expected_offset) {
				if (offset == *next_expected_offset) {
					sequential_read_calls++;
				} else if (offset > *next_expected_offset) {
					forward_seek_calls++;
					largest_forward_gap = MaxValue<idx_t>(largest_forward_gap, offset - *next_expected_offset);
				} else {
					backward_seek_calls++;
					largest_backward_gap = MaxValue<idx_t>(largest_backward_gap, *next_expected_offset - offset);
				}
			}
			next_expected_offset = offset + size;
		}

		void RecordSftpRead(idx_t handle_index, idx_t size) {
			sftp_read_calls++;
			sftp_bytes_read += size;
			handle_sftp_read_counts[handle_index]++;
			returned_size_histogram[size]++;
		}

		static void PrintHistogram(std::ostream &out, const char *label, const std::map<idx_t, idx_t> &histogram) {
			out << "  " << label << ":";
			if (histogram.empty()) {
				out << " <empty>\n";
				return;
			}
			out << '\n';
			for (const auto &entry : histogram) {
				out << "    " << entry.first << " bytes -> " << entry.second << " calls\n";
			}
		}

		void Print(const std::string &path) const {
			std::cerr << "H5DB SFTP profile for " << path << '\n';
			std::cerr << "  backend_read_calls=" << backend_read_calls << '\n';
			std::cerr << "  backend_bytes_requested=" << backend_bytes_requested << '\n';
			if (backend_read_calls > 0) {
				std::cerr << "  average_backend_read_size="
				          << static_cast<double>(backend_bytes_requested) / static_cast<double>(backend_read_calls)
				          << '\n';
			}
			std::cerr << "  sequential_read_calls=" << sequential_read_calls << '\n';
			std::cerr << "  forward_seek_calls=" << forward_seek_calls << '\n';
			std::cerr << "  backward_seek_calls=" << backward_seek_calls << '\n';
			std::cerr << "  largest_forward_gap=" << largest_forward_gap << '\n';
			std::cerr << "  largest_backward_gap=" << largest_backward_gap << '\n';
			std::cerr << "  open_handle_count=" << open_handle_count << '\n';
			std::cerr << "  sftp_read_calls=" << sftp_read_calls << '\n';
			std::cerr << "  sftp_bytes_read=" << sftp_bytes_read << '\n';
			if (sftp_read_calls > 0) {
				std::cerr << "  average_sftp_read_size="
				          << static_cast<double>(sftp_bytes_read) / static_cast<double>(sftp_read_calls) << '\n';
			}
			for (idx_t i = 0; i < SFTP_HANDLE_POOL_SIZE; i++) {
				if (handle_use_counts[i] == 0 && handle_sftp_read_counts[i] == 0) {
					continue;
				}
				std::cerr << "  handle[" << i << "] backend_reads=" << handle_use_counts[i]
				          << " sftp_reads=" << handle_sftp_read_counts[i] << '\n';
			}
			PrintHistogram(std::cerr, "backend request sizes", requested_size_histogram);
			PrintHistogram(std::cerr, "libssh2 read sizes", returned_size_histogram);
		}
	};

	void ThrowIfInterrupted() const {
		if (context && context->interrupted.load(std::memory_order_relaxed)) {
			throw InterruptException();
		}
	}

	void MarkConnectionDeadAfterFileEngineFailure() noexcept {
		// Conservative v1 policy: we invalidate the shared connection on any file-engine failure
		// instead of trying to distinguish file-local SFTP errors from transport/session failures here.
		// That boundary could be made more selective later, but with a query-local cache and fail-fast
		// query behavior the practical impact of over-invalidating is low.
		connection->MarkDead();
	}

	void CloseReadHandle(SftpReadHandle &handle_slot) noexcept {
		auto *handle = handle_slot.handle;
		handle_slot.handle = nullptr;
		handle_slot.current_offset.reset();
		if (!handle) {
			return;
		}
		try {
			while (true) {
				auto rc = libssh2_sftp_close_handle(handle);
				if (rc == 0) {
					return;
				}
				if (rc != LIBSSH2_ERROR_EAGAIN) {
					connection->MarkDead();
					return;
				}
				connection->WaitForSessionIO();
			}
		} catch (...) {
			connection->MarkDead();
		}
	}

	void ReadInternal(idx_t offset, idx_t size, void *buf) {
		connection->ThrowIfDead();
		auto &handle_slot = SelectReadHandle(offset);
		if (profiling_enabled) {
			stats.RecordRead(offset, size, handle_slot.index);
		}
		if (!handle_slot.current_offset || *handle_slot.current_offset != offset) {
			libssh2_sftp_seek64(handle_slot.handle, offset);
			handle_slot.current_offset = offset;
		}
		auto *out = static_cast<char *>(buf);
		idx_t remaining = size;
		while (remaining > 0) {
			ThrowIfInterrupted();
			auto nread = libssh2_sftp_read(handle_slot.handle, out, remaining);
			if (nread == LIBSSH2_ERROR_EAGAIN) {
				connection->WaitForSessionIO();
				continue;
			}
			if (nread < 0) {
				throw IOException("Failed to read SFTP data: %s", connection->LastSessionError());
			}
			if (nread == 0) {
				throw IOException("Unexpected EOF while reading SFTP file");
			}
			if (profiling_enabled) {
				stats.RecordSftpRead(handle_slot.index, UnsafeNumericCast<idx_t>(nread));
			}
			out += nread;
			auto read_size = UnsafeNumericCast<idx_t>(nread);
			remaining -= read_size;
			handle_slot.current_offset = *handle_slot.current_offset + read_size;
		}
	}

	SftpReadHandle &OpenReadHandle(idx_t handle_index) {
		connection->ThrowIfDead();
		auto &handle_slot = sftp_handles[handle_index];
		if (handle_slot.handle) {
			return handle_slot;
		}
		while (true) {
			handle_slot.handle =
			    libssh2_sftp_open(connection->GetSftpSession(), remote_path.c_str(), LIBSSH2_FXF_READ, 0);
			if (handle_slot.handle) {
				handle_slot.current_offset.reset();
				if (profiling_enabled) {
					stats.RecordHandleOpened();
				}
				return handle_slot;
			}
			if (libssh2_session_last_errno(connection->GetSession()) != LIBSSH2_ERROR_EAGAIN) {
				throw IOException("Failed to open SFTP file '%s': %s", remote_path, connection->LastSessionError());
			}
			connection->WaitForSessionIO();
		}
	}

	void StatFile() {
		connection->ThrowIfDead();
		LIBSSH2_SFTP_ATTRIBUTES attrs;
		while (true) {
			auto rc = libssh2_sftp_fstat_ex(sftp_handles[0].handle, &attrs, 0);
			if (rc == 0) {
				break;
			}
			if (rc != LIBSSH2_ERROR_EAGAIN) {
				throw IOException("Failed to stat SFTP file '%s': %s", remote_path, connection->LastSessionError());
			}
			connection->WaitForSessionIO();
		}
		if (!(attrs.flags & LIBSSH2_SFTP_ATTR_SIZE)) {
			throw IOException("SFTP file '%s' did not report a file size", remote_path);
		}
		file_size = UnsafeNumericCast<idx_t>(attrs.filesize);
		last_modified = (attrs.flags & LIBSSH2_SFTP_ATTR_ACMODTIME)
		                    ? Timestamp::FromEpochSeconds(UnsafeNumericCast<int64_t>(attrs.mtime))
		                    : timestamp_t();
	}

	SftpReadHandle &SelectReadHandle(idx_t offset) {
		SftpReadHandle *exact_match = nullptr;
		SftpReadHandle *nearest_local = nullptr;
		idx_t nearest_local_distance = NumericLimits<idx_t>::Maximum();
		SftpReadHandle *unopened = nullptr;
		SftpReadHandle *nearest_any = nullptr;
		idx_t nearest_any_distance = NumericLimits<idx_t>::Maximum();

		for (auto &handle_slot : sftp_handles) {
			if (!handle_slot.IsOpen()) {
				if (!unopened) {
					unopened = &handle_slot;
				}
				continue;
			}
			if (handle_slot.current_offset && *handle_slot.current_offset == offset) {
				exact_match = &handle_slot;
				break;
			}
			idx_t distance = 0;
			if (handle_slot.current_offset) {
				auto current = *handle_slot.current_offset;
				distance = current > offset ? current - offset : offset - current;
			}
			if (distance < nearest_any_distance) {
				nearest_any_distance = distance;
				nearest_any = &handle_slot;
			}
			if (distance <= SFTP_HANDLE_LOCALITY_THRESHOLD && distance < nearest_local_distance) {
				nearest_local_distance = distance;
				nearest_local = &handle_slot;
			}
		}

		if (exact_match) {
			return *exact_match;
		}
		if (nearest_local) {
			return *nearest_local;
		}
		if (unopened) {
			return OpenReadHandle(unopened->index);
		}
		D_ASSERT(nearest_any);
		return *nearest_any;
	}

	shared_ptr<H5SftpConnection> connection;
	ClientContext *context = nullptr;
	std::string remote_path;
	const bool profiling_enabled;
	SftpReadStats stats;
	std::array<SftpReadHandle, SFTP_HANDLE_POOL_SIZE> sftp_handles;
	idx_t file_size = 0;
	timestamp_t last_modified;
};

class H5SftpFileHandle : public FileHandle {
public:
	H5SftpFileHandle(FileSystem &fs, const OpenFileInfo &file, FileOpenFlags flags) : FileHandle(fs, file.path, flags) {
	}

	void Close() override {
	}

	idx_t position = 0;
};

class H5SftpFileSystem : public FileSystem {
public:
	H5SftpFileSystem(ClientContext &context_p, const H5SftpUrl &url_p)
	    : context(context_p), config(ResolveSftpConfig(context_p, url_p)), path(url_p.original_url) {
	}

protected:
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override {
		if (file.path != path) {
			throw InternalException("Unexpected SFTP file open for '%s' on handle bound to '%s'", file.path, path);
		}
		return make_uniq<H5SftpFileHandle>(*this, file, flags);
	}

	bool SupportsOpenFileExtended() const override {
		return true;
	}

public:
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		auto &sftp_handle = handle.Cast<H5SftpFileHandle>();
		GetOrCreateEngine().Read(location, UnsafeNumericCast<idx_t>(nr_bytes), buffer);
		sftp_handle.position = location + UnsafeNumericCast<idx_t>(nr_bytes);
	}

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		auto &sftp_handle = handle.Cast<H5SftpFileHandle>();
		GetOrCreateEngine().Read(sftp_handle.position, UnsafeNumericCast<idx_t>(nr_bytes), buffer);
		sftp_handle.position += UnsafeNumericCast<idx_t>(nr_bytes);
		return nr_bytes;
	}

	int64_t GetFileSize(FileHandle &handle) override {
		return UnsafeNumericCast<int64_t>(GetOrCreateEngine().GetFileSize());
	}

	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		return GetOrCreateEngine().GetLastModifiedTime();
	}

	string GetVersionTag(FileHandle &handle) override {
		return "";
	}

	void Seek(FileHandle &handle, idx_t location) override {
		handle.Cast<H5SftpFileHandle>().position = location;
	}

	idx_t SeekPosition(FileHandle &handle) override {
		return handle.Cast<H5SftpFileHandle>().position;
	}

	bool CanHandleFile(const string &fpath) override {
		return IsSftpPath(fpath);
	}

	bool CanSeek() override {
		return true;
	}

	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}

	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override {
		return false;
	}

	string GetName() const override {
		return "H5SftpFileSystem";
	}

	string PathSeparator(const string &path) override {
		return "/";
	}

private:
	H5SftpFileEngine &GetOrCreateEngine() {
		if (!engine) {
			auto connection = GetOrCreateCachedSftpConnection(context, config);
			engine = make_uniq<H5SftpFileEngine>(std::move(connection), config);
		}
		return *engine;
	}

	ClientContext &context;
	H5SftpConfig config;
	std::string path;
	unique_ptr<H5SftpFileEngine> engine;
};

class H5SftpRemoteBackend : public H5RemoteBackend {
public:
	H5SftpRemoteBackend(ClientContext &context_p, const H5SftpUrl &url_p)
	    : raw_fs(context_p, url_p), caching_fs(raw_fs, *context_p.db), path(url_p.original_url),
	      read_flags(FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO) {
		OpenFileInfo open_info(path);
		if (Settings::Get<ValidateExternalFileCacheSetting>(context_p) == CacheValidationMode::VALIDATE_REMOTE) {
			open_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
			open_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(true);
		}
		cached_handle = caching_fs.OpenFile(QueryContext(context_p), open_info, read_flags);
		if (!cached_handle) {
			throw IOException("Failed to open remote SFTP file '%s'", path);
		}
		file_size = cached_handle->GetFileSize();
	}

	idx_t GetFileSize() const override {
		return file_size;
	}

	void ReadCached(idx_t offset, idx_t size, void *buf) override {
		if (!cached_handle) {
			throw IOException("Failed to read SFTP data: no readable cached file handle");
		}
		data_ptr_t buffer = nullptr;
		auto pinned_buffer = cached_handle->Read(buffer, size, offset);
		if (!pinned_buffer.IsValid() || !buffer) {
			throw IOException("Failed to read SFTP data: invalid cached buffer");
		}
		std::memcpy(buf, buffer, size);
	}

	void ReadDirect(idx_t offset, idx_t size, void *buf) override {
		OpenDirectHandleIfNeeded();
		if (!direct_handle) {
			throw IOException("Failed to read SFTP data: no readable direct file handle");
		}
		direct_handle->Read(buf, size, offset);
	}

private:
	void OpenDirectHandleIfNeeded() {
		if (direct_handle) {
			return;
		}
		OpenFileInfo open_info(path);
		direct_handle = raw_fs.OpenFile(open_info, read_flags);
		if (!direct_handle) {
			throw IOException("Failed to open remote SFTP file '%s' for direct reads", path);
		}
	}

	H5SftpFileSystem raw_fs;
	CachingFileSystem caching_fs;
	std::string path;
	FileOpenFlags read_flags;
	unique_ptr<CachingFileHandle> cached_handle;
	unique_ptr<FileHandle> direct_handle;
	idx_t file_size = 0;
};

} // namespace

bool IsH5RemotePath(const std::string &path) {
	return IsSftpPath(path) || FileSystem::IsRemoteFile(path);
}

H5RemoteBackendDescriptor DescribeH5RemotePath(const std::string &path) {
	if (IsSftpPath(path)) {
		return {H5RemoteBackendType::SFTP, ""};
	}
	std::string required_extension;
	if (TryGetDuckDBFsRemoteExtension(path, required_extension)) {
		return {H5RemoteBackendType::DUCKDB_FS, required_extension};
	}
	throw InvalidInputException("Not a supported remote path: %s", path);
}

unique_ptr<H5RemoteBackend> OpenH5RemoteBackend(ClientContext &context, const std::string &path) {
	auto descriptor = DescribeH5RemotePath(path);
	switch (descriptor.type) {
	case H5RemoteBackendType::DUCKDB_FS:
		return make_uniq<DuckDBFsRemoteBackend>(context, path);
	case H5RemoteBackendType::SFTP:
		return make_uniq<H5SftpRemoteBackend>(context, ParseSftpUrl(path));
	default:
		throw InternalException("Unknown remote backend type");
	}
}

H5ExpandedFileList ExpandH5SftpFilePattern(ClientContext &context, const std::string &path_pattern) {
	H5ExpandedFileList result;
	auto url = ParseSftpUrl(path_pattern);
	auto pattern = ParseSftpRemotePattern(url.remote_path);
	if (!pattern.has_glob) {
		result.filenames.push_back(path_pattern);
		return result;
	}

	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
	auto config = ResolveSftpConfig(context, url);
	auto connection = GetOrCreateCachedSftpConnection(context, config);
	result.filenames = H5SftpGlobExpander(std::move(connection), url.authority, std::move(pattern)).Expand();
	if (result.filenames.empty()) {
		throw IOException("No files found that match the pattern \"%s\"", path_pattern);
	}
	result.had_glob = true;
	return result;
}

} // namespace duckdb
