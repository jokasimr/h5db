#include "h5_functions.hpp"

#include "duckdb/main/secret/secret.hpp"

namespace duckdb {

static unique_ptr<BaseSecret> CreateH5SftpSecretFromConfig(ClientContext &, CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);

	secret->TrySetValue("username", input);
	secret->TrySetValue("password", input);
	secret->TrySetValue("key_path", input);
	secret->TrySetValue("key_passphrase", input);
	secret->TrySetValue("port", input);
	secret->TrySetValue("known_hosts_path", input);
	secret->TrySetValue("host_key_fingerprint", input);
	secret->TrySetValue("host_key_algorithms", input);

	Value value;
	if (!secret->TryGetValue("username", value)) {
		throw InvalidInputException("sftp secret requires USERNAME");
	}

	auto has_password = secret->TryGetValue("password", value);
	auto has_key_path = secret->TryGetValue("key_path", value);
	if (!has_password && !has_key_path) {
		throw InvalidInputException("sftp secret requires either PASSWORD or KEY_PATH");
	}
	if (has_password && has_key_path) {
		throw InvalidInputException("sftp secret requires exactly one of PASSWORD or KEY_PATH");
	}

	auto has_known_hosts = secret->TryGetValue("known_hosts_path", value);
	auto has_host_key_fingerprint = secret->TryGetValue("host_key_fingerprint", value);
	if (!has_known_hosts && !has_host_key_fingerprint) {
		throw InvalidInputException("sftp secret requires either KNOWN_HOSTS_PATH or HOST_KEY_FINGERPRINT");
	}
	if (secret->TryGetValue("port", value)) {
		auto port = value.GetValue<int32_t>();
		if (port < 1 || port > 65535) {
			throw InvalidInputException("sftp secret PORT must be between 1 and 65535");
		}
	}

	secret->redact_keys = {"password", "key_passphrase"};
	return std::move(secret);
}

void RegisterH5SftpSecrets(ExtensionLoader &loader) {
	SecretType secret_type;
	secret_type.name = "sftp";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	secret_type.extension = "h5db";
	loader.RegisterSecretType(std::move(secret_type));

	CreateSecretFunction config_fun;
	config_fun.secret_type = "sftp";
	config_fun.provider = "config";
	config_fun.function = CreateH5SftpSecretFromConfig;
	config_fun.named_parameters["username"] = LogicalType::VARCHAR;
	config_fun.named_parameters["password"] = LogicalType::VARCHAR;
	config_fun.named_parameters["key_path"] = LogicalType::VARCHAR;
	config_fun.named_parameters["key_passphrase"] = LogicalType::VARCHAR;
	config_fun.named_parameters["port"] = LogicalType::INTEGER;
	config_fun.named_parameters["known_hosts_path"] = LogicalType::VARCHAR;
	config_fun.named_parameters["host_key_fingerprint"] = LogicalType::VARCHAR;
	config_fun.named_parameters["host_key_algorithms"] = LogicalType::VARCHAR;
	loader.RegisterFunction(std::move(config_fun));
}

} // namespace duckdb
