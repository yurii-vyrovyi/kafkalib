package kafkalib

import (
	"errors"
	"time"

	confluentkafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type (
	ConsumerConfig struct {
		Topics              []string `json:"topics" yaml:"topics" split_words:"true" envconfig:"TOPICS"`
		Brokers             string   `json:"brokers" yaml:"brokers" split_words:"true" envconfig:"BROKERS"`
		GroupID             string   `json:"group_id" yaml:"group-id" split_words:"true" envconfig:"GROUP_ID"`
		OffsetResetEarliest bool     `json:"offset_reset_earliest" yaml:"offset-reset-earliest" envconfig:"OFFSET_RESET_EARLIEST"`

		SSLEnabled             bool   `json:"ssl_enabled" yaml:"ssl-enabled" envconfig:"SSL_ENABLED"`
		ClientKeyFilePath      string `json:"client_key_file_path" yaml:"client-key-file-path" envconfig:"CLIENT_KEY_FILE_PATH"`
		ClientCertFilePath     string `json:"client_cert_file_path" yaml:"client-cert-file-path" envconfig:"CLIENT_CERT_FILE_PATH"`
		CaFilePath             string `json:"ca_file_path" yaml:"ca-file-path" envconfig:"CA_FILE_PATH"`
		EnableCertVerification bool   `json:"enable_cert_verification" yaml:"enable-cert-verification" envconfig:"ENABLE_CERT_VERIFICATION"`
		SaslUser               string `json:"sasl_user" yaml:"sasl-user" envconfig:"SASL_USER"`
		SaslPassword           string `json:"sasl_password" yaml:"sasl-password" envconfig:"SASL_PASSWORD"`

		// Ping timeout
		PingTimeout time.Duration `json:"ping_timeout" yaml:"ping-timeout" envconfig:"PING_TIMEOUT"`

		// CheckConnectionTimeout defines how long client will retry to connect brokers on start
		CheckConnectionTimeout time.Duration `json:"check_connection_timeout" yaml:"check-connection-timeout" envconfig:"CHECK_CONNECTION_TIMEOUT"`

		// SessionTimeoutMs defines session.timeout.ms config
		SessionTimeoutMs int `json:"session_timeout_ms" yaml:"session-timeout-ms" envconfig:"SESSION_TIMEOUT_MS"`

		// EnableLogs enables putting consumer logs to debug logger
		EnableLogs bool `json:"enable_logs" yaml:"enable-logs" envconfig:"ENABLE_LOGS"`

		// CommitPeriod is period between committing offsets
		CommitPeriod time.Duration `json:"commit_period" yaml:"commit-period" envconfig:"COMMIT_PERIOD"`

		// CloseTimeout defines a timeout limit for consumer Close()
		CloseTimeout time.Duration `json:"close_timeout" yaml:"close-timeout" envconfig:"CLOSE_TIMEOUT"`

		// CommitFaultsTolerance defines how many times commit should fail consequently to raise an error.
		CommitFaultsTolerance int `json:"commit_faults_tolerance" yaml:"commit-faults-tolerance" envconfig:"COMMIT_FAULTS_TOLERANCE"`
	}
)

const (
	defaultConsumerEnablePartitionEOF    = true
	defaultConsumerEnableAutoCommit      = false
	defaultConsumerAppPartitionRebalance = true
	defaultConsumerPingTimeout           = 10 * time.Second
	defaultConsumerConnectionTimeout     = 60 * time.Second
	defaultCommitPeriod                  = 10 * time.Second
	defaultCloseTimeout                  = 20 * time.Second
	defaultCommitFaultsTolerance         = 3

	offsetConsumerResetEarliest = "earliest"
	offsetConsumerResetLatest   = "latest"
)

// WithDefaults returns a consumer config. If original values are invalid it replaces it with default ones.
func (config *ConsumerConfig) WithDefaults() ConsumerConfig {
	retConfig := *config

	if retConfig.PingTimeout == 0 {
		retConfig.PingTimeout = defaultConsumerPingTimeout
	}

	if retConfig.CheckConnectionTimeout == 0 {
		retConfig.CheckConnectionTimeout = defaultConsumerConnectionTimeout
	}

	if retConfig.CommitPeriod == 0 {
		retConfig.CommitPeriod = defaultCommitPeriod
	}

	if retConfig.CloseTimeout == 0 {
		retConfig.CloseTimeout = defaultCloseTimeout
	}

	if retConfig.CommitFaultsTolerance == 0 {
		config.CommitFaultsTolerance = defaultCommitFaultsTolerance
	}

	return retConfig
}

// ConfigMapWithDefaults returns confluent Kafka config map that is necessary to run consumer correctly.
func (config *ConsumerConfig) ConfigMapWithDefaults() confluentkafka.ConfigMap {

	configMap := confluentkafka.ConfigMap{
		"bootstrap.servers": config.Brokers,
		"group.id":          config.GroupID,
		"auto.offset.reset": config.offsetReset(),

		"enable.auto.commit":              defaultConsumerEnableAutoCommit,
		"enable.partition.eof":            defaultConsumerEnablePartitionEOF,
		"go.application.rebalance.enable": defaultConsumerAppPartitionRebalance,

		"go.events.channel.enable": true,
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

	if config.SessionTimeoutMs > 0 {
		configMap["session.timeout.ms"] = config.SessionTimeoutMs
	}

	if config.EnableLogs {
		configMap["go.logs.channel.enable"] = true
		configMap["debug"] = "all"
	}

	return configMap
}

func (config *ConsumerConfig) validate() error {
	if config.Brokers == "" {
		return errors.New("kafka: missing broker configuration")
	}

	if len(config.Topics) == 0 {
		return errors.New("kafka: missing broker configuration")
	}

	return nil
}

func (config *ConsumerConfig) offsetReset() string {
	if config.OffsetResetEarliest {
		return offsetConsumerResetEarliest
	}

	return offsetConsumerResetLatest
}
