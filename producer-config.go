package kafkalib

import (
	"time"

	confluentkafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type (
	ProducerConfig struct {
		Brokers               string        `json:"brokers" yaml:"brokers" envconfig:"BROKERS" validate:"required"`
		FlushTimeout          time.Duration `json:"flush_timeout" yaml:"flush-timeout" envconfig:"FLUSH_TIMEOUT"`
		PingTimeout           time.Duration `json:"ping_timeout" yaml:"ping-timeout" envconfig:"PING_TIMEOUT"`
		EnableLogs            bool          `json:"enable_logs" yaml:"enable-logs" envconfig:"ENABLE_LOGS"`
		ValidateMessageSchema bool          `json:"validate_message" yaml:"validate-message" envconfig:"VALIDATE_MESSAGE"`
		SchemaTTL             time.Duration `json:"schema_ttl" yaml:"schema-ttl" envconfig:"SCHEMA_TTL"`
		SchemaRegistryTimeout time.Duration `json:"schema_registry_timeout" yaml:"schema-registry-timeout" envconfig:"SCHEMA_REGISTRY_TIMEOUT"`

		SSLEnabled             bool   `json:"ssl_enabled" yaml:"ssl-enabled" envconfig:"SSL_ENABLED"`
		ClientKeyFilePath      string `json:"client_key_file_path" yaml:"client-key-file-path" envconfig:"CLIENT_KEY_FILE_PATH"`
		ClientCertFilePath     string `json:"client_cert_file_path" yaml:"client-cert-file-path" envconfig:"CLIENT_CERT_FILE_PATH"`
		CaFilePath             string `json:"ca_file_path" yaml:"ca-file-path" envconfig:"CA_FILE_PATH"`
		EnableCertVerification bool   `json:"enable_cert_verification" yaml:"enable-cert-verification" envconfig:"ENABLE_CERT_VERIFICATION"`
		SaslUser               string `json:"sasl_user" yaml:"sasl-user" envconfig:"SASL_USER"`
		SaslPassword           string `json:"sasl_password" yaml:"sasl-password" envconfig:"SASL_PASSWORD"`
	}
)

const (
	SecurityProtocolSsl     = "SSL"
	SecurityProtocolSaslSsl = "SASL_SSL"

	SaslMechanismScramSha256 = "SCRAM-SHA-256"
)

const (
	defaultSchemaRegistryTimeout = 10 * time.Second
	defaultFlushTimeout          = 15 * time.Second
	defaultPingTimeout           = 10 * time.Second
	defaultSchemaTTL             = 60 * time.Second
)

// WithDefaults returns config with default values for the ones that are not set.
func (config *ProducerConfig) WithDefaults() ProducerConfig {
	retConfig := *config

	if retConfig.FlushTimeout == 0 {
		retConfig.FlushTimeout = defaultFlushTimeout
	}

	if retConfig.PingTimeout == 0 {
		retConfig.PingTimeout = defaultPingTimeout
	}

	if retConfig.SchemaTTL == 0 {
		retConfig.SchemaTTL = defaultSchemaTTL
	}

	if retConfig.SchemaRegistryTimeout == 0 {
		retConfig.SchemaRegistryTimeout = defaultSchemaRegistryTimeout
	}

	return retConfig
}

// ConfigMapWithDefaults returns confluent Kafka config map that is necessary to run producer correctly.
func (config *ProducerConfig) ConfigMapWithDefaults() *confluentkafka.ConfigMap {
	configMap := confluentkafka.ConfigMap{
		"bootstrap.servers":  config.Brokers,
		"partitioner":        "murmur2_random", // compatible to java client
		"enable.idempotence": true,             // keep order
	}

	if config.EnableLogs {
		configMap["go.logs.channel.enable"] = true
		configMap["debug"] = "all"
	}

	if config.SSLEnabled {
		configMap["security.protocol"] = SecurityProtocolSaslSsl
		configMap["sasl.mechanism"] = SaslMechanismScramSha256

		configMap["ssl.key.location"] = config.ClientKeyFilePath
		configMap["ssl.certificate.location"] = config.ClientCertFilePath
		configMap["ssl.ca.location"] = config.CaFilePath

		configMap["enable.ssl.certificate.verification"] = config.EnableCertVerification
		configMap["sasl.username"] = config.SaslUser
		configMap["sasl.password"] = config.SaslPassword
	}

	return &configMap
}
