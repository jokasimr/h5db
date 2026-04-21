#include "h5_remote_backend.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/path.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/caching_file_system.hpp"

#ifndef _WIN32
#include <libssh2.h>
#include <libssh2_sftp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#endif

#include <cstring>
#include <cstdlib>
#include <array>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
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

#ifndef _WIN32
struct H5SftpUrl {
	std::string original_url;
	std::string authority;
	std::string host;
	std::string remote_path;
	std::optional<std::string> username;
	std::optional<int> port;
};

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
	std::string secret_scope_cache_key;
	std::string remote_path;
};

struct SftpHostKeyHintCacheKey {
	std::string scope_key;
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

static std::mutex sftp_host_key_hint_lock;
static std::map<SftpHostKeyHintCacheKey, std::string> sftp_host_key_hint_cache;

static std::string BuildSecretScopeCacheKey(const BaseSecret &secret) {
	std::ostringstream result;
	result << secret.GetName() << '\n';
	for (const auto &scope : secret.GetScope()) {
		result << scope << '\n';
	}
	return result.str();
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

static std::string JoinPathSegments(const Path &path) {
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
	result.remote_path = JoinPathSegments(parsed);

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

class ScopedSocket {
public:
	explicit ScopedSocket(int fd_p) : fd(fd_p) {
	}
	~ScopedSocket() {
		if (fd >= 0) {
			close(fd);
		}
	}
	int Get() const {
		return fd;
	}

private:
	int fd;
};

class H5SftpFileEngine {
public:
	H5SftpFileEngine(ClientContext &context_p, const H5SftpUrl &url_p) : context(&context_p), url(url_p) {
		config = ResolveSftpConfig(context_p, url);
		InitializeLibssh2();
		OpenVerifiedSession();
		Authenticate();
		OpenSftpHandle();
		StatFile();
	}

	idx_t GetFileSize() const {
		return file_size;
	}

	timestamp_t GetLastModifiedTime() const {
		return last_modified;
	}

	void Read(idx_t offset, idx_t size, void *buf) {
		ReadInternal(offset, size, buf);
	}

private:
	void ReadInternal(idx_t offset, idx_t size, void *buf) {
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
				WaitForSessionIO();
				continue;
			}
			if (nread < 0) {
				throw IOException("Failed to read SFTP data: %s", LastSessionError());
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

public:
	~H5SftpFileEngine() {
		if (profiling_enabled) {
			stats.Print(config.remote_path);
		}
		ResetConnectionState();
	}

private:
	static constexpr idx_t SFTP_HANDLE_POOL_SIZE = 4;
	static constexpr idx_t SFTP_HANDLE_LOCALITY_THRESHOLD = 2 * 1024 * 1024;
	static constexpr int IO_WAIT_SLICE_MS = 100;

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

	static std::vector<std::string> GetSupportedHostKeyAlgorithms() {
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

		{
			std::lock_guard<std::mutex> guard(sftp_host_key_hint_lock);
			auto lookup = sftp_host_key_hint_cache.find(GetHostKeyHintCacheKey());
			if (lookup != sftp_host_key_hint_cache.end()) {
				add_attempt(lookup->second);
			}
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
		for (auto &handle_slot : sftp_handles) {
			if (handle_slot.handle) {
				libssh2_sftp_close_handle(handle_slot.handle);
				handle_slot.handle = nullptr;
			}
			handle_slot.current_offset.reset();
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

	void WaitForSocketEvents(short events) {
		D_ASSERT(socket_fd);
		struct pollfd pfd;
		pfd.fd = socket_fd->Get();
		pfd.events = events;
		pfd.revents = 0;

		while (true) {
			ThrowIfInterrupted();
			auto rc = poll(&pfd, 1, IO_WAIT_SLICE_MS);
			if (rc > 0) {
				return;
			}
			if (rc == 0) {
				continue;
			}
			if (errno == EINTR) {
				continue;
			}
			throw IOException("Polling SFTP socket failed: %s", std::strerror(errno));
		}
	}

	void WaitForSessionIO() {
		D_ASSERT(session);
		auto directions = libssh2_session_block_directions(session);
		short events = 0;
		if (directions & LIBSSH2_SESSION_BLOCK_INBOUND) {
			events |= POLLIN;
		}
		if (directions & LIBSSH2_SESSION_BLOCK_OUTBOUND) {
			events |= POLLOUT;
		}
		if (events == 0) {
			events = POLLIN | POLLOUT;
		}
		WaitForSocketEvents(events);
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
			throw IOException("Failed to resolve SFTP host '%s': %s", config.host, gai_strerror(rc));
		}

		std::unique_ptr<struct addrinfo, decltype(&freeaddrinfo)> addrinfo_guard(result, freeaddrinfo);
		int fd = -1;
		for (auto *rp = result; rp; rp = rp->ai_next) {
			ThrowIfInterrupted();
			fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
			if (fd < 0) {
				continue;
			}
			auto flags = fcntl(fd, F_GETFL, 0);
			if (flags < 0) {
				close(fd);
				fd = -1;
				continue;
			}
			if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
				close(fd);
				fd = -1;
				continue;
			}
			if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
				socket_fd = make_uniq<ScopedSocket>(fd);
				return;
			}
			if (errno == EINPROGRESS || errno == EWOULDBLOCK) {
				socket_fd = make_uniq<ScopedSocket>(fd);
				WaitForSocketEvents(POLLOUT);

				int socket_error = 0;
				socklen_t error_len = sizeof(socket_error);
				if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &error_len) == 0 && socket_error == 0) {
					return;
				}

				socket_fd.reset();
				fd = -1;
				continue;
			}
			close(fd);
			fd = -1;
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
				std::lock_guard<std::mutex> guard(sftp_host_key_hint_lock);
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
		while (true) {
			auto rc = libssh2_userauth_publickey_fromfile(session, config.username.c_str(), nullptr,
			                                              config.key_path->c_str(), passphrase);
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

	SftpReadHandle &OpenReadHandle(idx_t handle_index) {
		auto &handle_slot = sftp_handles[handle_index];
		if (handle_slot.handle) {
			return handle_slot;
		}
		while (true) {
			handle_slot.handle = libssh2_sftp_open(sftp_session, config.remote_path.c_str(), LIBSSH2_FXF_READ, 0);
			if (handle_slot.handle) {
				handle_slot.current_offset.reset();
				if (profiling_enabled) {
					stats.RecordHandleOpened();
				}
				return handle_slot;
			}
			if (libssh2_session_last_errno(session) != LIBSSH2_ERROR_EAGAIN) {
				throw IOException("Failed to open SFTP file '%s': %s", config.remote_path, LastSessionError());
			}
			WaitForSessionIO();
		}
	}

	void OpenSftpHandle() {
		while (true) {
			sftp_session = libssh2_sftp_init(session);
			if (sftp_session) {
				break;
			}
			if (libssh2_session_last_errno(session) != LIBSSH2_ERROR_EAGAIN) {
				throw IOException("Failed to initialize SFTP session: %s", LastSessionError());
			}
			WaitForSessionIO();
		}
		for (idx_t i = 0; i < sftp_handles.size(); i++) {
			sftp_handles[i].index = i;
		}
		OpenReadHandle(0);
	}

	void StatFile() {
		LIBSSH2_SFTP_ATTRIBUTES attrs;
		while (true) {
			auto rc = libssh2_sftp_fstat_ex(sftp_handles[0].handle, &attrs, 0);
			if (rc == 0) {
				break;
			}
			if (rc != LIBSSH2_ERROR_EAGAIN) {
				throw IOException("Failed to stat SFTP file '%s': %s", config.remote_path, LastSessionError());
			}
			WaitForSessionIO();
		}
		if (!(attrs.flags & LIBSSH2_SFTP_ATTR_SIZE)) {
			throw IOException("SFTP file '%s' did not report a file size", config.remote_path);
		}
		file_size = UnsafeNumericCast<idx_t>(attrs.filesize);
		last_modified =
		    (attrs.flags & LIBSSH2_SFTP_ATTR_ACMODTIME) ? Timestamp::FromEpochSeconds(attrs.mtime) : timestamp_t();
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

	std::string LastSessionError() const {
		char *message = nullptr;
		int message_len = 0;
		libssh2_session_last_error(session, &message, &message_len, 0);
		if (message && message_len > 0) {
			return std::string(message, UnsafeNumericCast<size_t>(message_len));
		}
		return "unknown libssh2 error";
	}

	H5SftpUrl url;
	H5SftpConfig config;
	ClientContext *context = nullptr;
	const bool profiling_enabled = std::getenv("H5DB_SFTP_PROFILE") != nullptr;
	SftpReadStats stats;
	unique_ptr<ScopedSocket> socket_fd;
	LIBSSH2_SESSION *session = nullptr;
	LIBSSH2_SFTP *sftp_session = nullptr;
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
	    : context(context_p), url(url_p), path(url_p.original_url) {
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
			engine = make_shared_ptr<H5SftpFileEngine>(context, url);
		}
		return *engine;
	}

	ClientContext &context;
	H5SftpUrl url;
	std::string path;
	shared_ptr<H5SftpFileEngine> engine;
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
#endif

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
#ifdef _WIN32
		throw NotImplementedException("SFTP URLs are not supported on Windows");
#else
		return make_uniq<H5SftpRemoteBackend>(context, ParseSftpUrl(path));
#endif
	default:
		throw InternalException("Unknown remote backend type");
	}
}

} // namespace duckdb
