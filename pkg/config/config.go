// Package config загружает конфигурацию сервисов из переменных окружения.
// Секреты (пароли БД, JWT-секрет) берутся только отсюда и никогда не
// попадают в код или логи.
package config

import "time"

// HTTP описывает параметры встроенного HTTP-сервера.
type HTTP struct {
	Port         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

// Addr возвращает адрес прослушивания в формате, понятном net/http.
func (h HTTP) Addr() string { return ":" + h.Port }

// Postgres описывает подключение к базе данных сервиса.
type Postgres struct {
	DSN             string
	MaxConns        int32
	MinConns        int32
	ConnectTimeout  time.Duration
	MigrateOnStart  bool
	MigrateLockWait time.Duration
}

// Kafka описывает подключение к брокеру и поведение консьюмеров.
type Kafka struct {
	Brokers []string
	// GroupID — базовое имя consumer-группы сервиса.
	GroupID string
	// MaxRetries — число повторов обработки сообщения до отправки в DLQ.
	MaxRetries int
	// RetryBackoff — стартовая задержка между повторами (растёт экспоненциально).
	RetryBackoff    time.Duration
	MaxRetryBackoff time.Duration
	// DLQSuffix добавляется к имени топика для dead-letter очереди.
	DLQSuffix string
	// Partitions используется при создании топиков.
	Partitions        int
	ReplicationFactor int
	// WriteTimeout ограничивает время публикации одного батча.
	WriteTimeout time.Duration
	// CommitInterval = 0 означает синхронный ручной commit offset.
	MinBytes int
	MaxBytes int
}

// JWT описывает параметры выпуска и проверки токенов.
type JWT struct {
	Secret string
	TTL    time.Duration
	Issuer string
}

// Outbox описывает поведение фонового релеера transactional outbox.
type Outbox struct {
	PollInterval time.Duration
	BatchSize    int
	MaxAttempts  int
	CleanupAfter time.Duration
}

// App — общая конфигурация любого сервиса монорепозитория.
type App struct {
	ServiceName     string
	Env             string
	LogLevel        string
	ShutdownTimeout time.Duration
	HTTP            HTTP
	Postgres        Postgres
	Kafka           Kafka
	JWT             JWT
	Outbox          Outbox
	// Service содержит настройки, нужные лишь отдельным сервисам.
	Service ServiceSpecific
}

// ServiceSpecific — настройки, относящиеся к конкретным сервисам.
// Собраны в одном месте, чтобы список переменных окружения не растекался.
type ServiceSpecific struct {
	// AutoComplete (order-service): автоматический переход RESERVED -> COMPLETED
	// сразу после успешного резерва. Выключите, чтобы отработать отмену
	// заказа после резерва и компенсацию остатка.
	AutoComplete bool
	// BcryptCost (user-service): стоимость хеширования пароля.
	BcryptCost int
	// BootstrapAdminEmail/Password (user-service): если заданы, при старте
	// создаётся пользователь с ролью ADMIN. Нужен, потому что регистрация
	// всегда выдаёт роль USER.
	BootstrapAdminEmail    string
	BootstrapAdminPassword string
}

// Load читает конфигурацию для сервиса с именем service.
// defaultPort используется, если HTTP_PORT не задан.
func Load(service, defaultPort string) (App, error) {
	l := &loader{}

	cfg := App{
		ServiceName:     service,
		Env:             l.str("APP_ENV", "local"),
		LogLevel:        l.str("LOG_LEVEL", "info"),
		ShutdownTimeout: l.duration("SHUTDOWN_TIMEOUT", 20*time.Second),
		HTTP: HTTP{
			Port:         l.str("HTTP_PORT", defaultPort),
			ReadTimeout:  l.duration("HTTP_READ_TIMEOUT", 10*time.Second),
			WriteTimeout: l.duration("HTTP_WRITE_TIMEOUT", 15*time.Second),
			IdleTimeout:  l.duration("HTTP_IDLE_TIMEOUT", 60*time.Second),
		},
		Postgres: Postgres{
			DSN:             l.required("POSTGRES_DSN"),
			MaxConns:        int32(l.positiveInt("POSTGRES_MAX_CONNS", 10)),
			MinConns:        int32(l.intVal("POSTGRES_MIN_CONNS", 1)),
			ConnectTimeout:  l.duration("POSTGRES_CONNECT_TIMEOUT", 30*time.Second),
			MigrateOnStart:  l.boolVal("POSTGRES_MIGRATE_ON_START", true),
			MigrateLockWait: l.duration("POSTGRES_MIGRATE_LOCK_WAIT", 30*time.Second),
		},
		Kafka: Kafka{
			Brokers:           l.csv("KAFKA_BROKERS", []string{"localhost:9092"}),
			GroupID:           l.str("KAFKA_GROUP_ID", service),
			MaxRetries:        l.positiveInt("KAFKA_MAX_RETRIES", 3),
			RetryBackoff:      l.duration("KAFKA_RETRY_BACKOFF", 500*time.Millisecond),
			MaxRetryBackoff:   l.duration("KAFKA_MAX_RETRY_BACKOFF", 10*time.Second),
			DLQSuffix:         l.str("KAFKA_DLQ_SUFFIX", ".dlq"),
			Partitions:        l.positiveInt("KAFKA_PARTITIONS", 3),
			ReplicationFactor: l.positiveInt("KAFKA_REPLICATION_FACTOR", 1),
			WriteTimeout:      l.duration("KAFKA_WRITE_TIMEOUT", 10*time.Second),
			MinBytes:          l.positiveInt("KAFKA_MIN_BYTES", 1),
			MaxBytes:          l.positiveInt("KAFKA_MAX_BYTES", 10<<20),
		},
		JWT: JWT{
			Secret: l.secret("JWT_SECRET", 16),
			TTL:    l.duration("JWT_TTL", 24*time.Hour),
			Issuer: l.str("JWT_ISSUER", "user-service"),
		},
		Outbox: Outbox{
			PollInterval: l.duration("OUTBOX_POLL_INTERVAL", time.Second),
			BatchSize:    l.positiveInt("OUTBOX_BATCH_SIZE", 100),
			MaxAttempts:  l.positiveInt("OUTBOX_MAX_ATTEMPTS", 10),
			CleanupAfter: l.duration("OUTBOX_CLEANUP_AFTER", 72*time.Hour),
		},
		Service: ServiceSpecific{
			AutoComplete:           l.boolVal("ORDER_AUTO_COMPLETE", true),
			BcryptCost:             l.positiveInt("BCRYPT_COST", 10),
			BootstrapAdminEmail:    l.str("BOOTSTRAP_ADMIN_EMAIL", ""),
			BootstrapAdminPassword: l.str("BOOTSTRAP_ADMIN_PASSWORD", ""),
		},
	}

	return cfg, l.err()
}
