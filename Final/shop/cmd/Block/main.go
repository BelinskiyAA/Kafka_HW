package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
	"github.com/lovoo/goka"
)

// Значения по умолчанию
const (
	defaultBroker       = "kafka-1st-1:1092"
	defaultTopic        = "block-product"
	defaultUsername     = "admin"
	defaultPassword     = "admin-secret"
	defaultCaPath       = "/usr/local/bin/ca.crt"
)

func main() {
	// Загружаем .env (если есть)
	if err := godotenv.Load(); err != nil {
		fmt.Println("No .env file found, using defaults or system env")
	}

	// Читаем переменные окружения
	broker := getEnv("BROKER", defaultBroker)
	topic := getEnv("TOPIC", defaultTopic)
	username := getEnv("SASL_USERNAME", defaultUsername)
	password := getEnv("SASL_PASSWORD", defaultPassword)
	caPath := getEnv("CA_PATH", defaultCaPath)

	// Парсим флаги командной строки (переопределяют env)
	cmd := flag.String("cmd", "none", "add or rm product from block")
	word := flag.String("product", "", "product")
	flag.Parse()

	trimWord := strings.TrimSpace(*word)
	if trimWord == "" {
		fmt.Println("Empty 'product' parameter value")
		return
	}

	var censorWord Word
	censorWord.Word = trimWord

	switch *cmd {
	case "add", "rm":
		censorWord.Cmd = *cmd
	default:
		fmt.Println("Wrong 'cmd' parameter value")
		return
	}

	// Настраиваем глобальную конфигурацию goka с параметрами из env
	if err := configureGoka(username, password, caPath); err != nil {
		fmt.Printf("Failed to configure Goka: %v", err)
		return
	}

	// Создаём эмиттер с полученным брокером и топиком
	brokers := []string{broker}
	emitter, err := goka.NewEmitter(brokers, goka.Stream(topic), new(WordCodec))
	if err != nil {
		panic(fmt.Sprintf("error creating Kafka producer: %v", err))
	}
	defer emitter.Finish()

	key := "censor"
	if err = emitter.EmitSync(key, censorWord); err != nil {
		fmt.Printf("error: %v", err)
		return
	}
	fmt.Println("OK")
}

// configureGoka задаёт глобальные настройки Sarama для всех компонентов goka
func configureGoka(username, password, caPath string) error {
	cfg := goka.DefaultConfig()

	// Указываем версию протокола (подберите под свой кластер)
	cfg.Version = sarama.V2_8_0_0

	// Настройка TLS
	tlsConfig, err := loadTLSConfig(caPath)
	if err != nil {
		return fmt.Errorf("failed to load TLS config: %w", err)
	}
	cfg.Net.TLS.Enable = true
	cfg.Net.TLS.Config = tlsConfig

	// Настройка SASL (PLAIN)
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	cfg.Net.SASL.User = username
	cfg.Net.SASL.Password = password

	// Для корректной работы EmitSync
	cfg.Producer.Return.Successes = true
	cfg.Consumer.Return.Errors = true

	goka.ReplaceGlobalConfig(cfg)
	return nil
}

// loadTLSConfig загружает CA-сертификат; при ошибке использует InsecureSkipVerify
func loadTLSConfig(caPath string) (*tls.Config, error) {
	cfg := &tls.Config{
		InsecureSkipVerify: true, // запасной вариант
	}
	if caPath == "" {
		return cfg, nil
	}
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		// Если файл не найден – оставляем InsecureSkipVerify (только для тестов)
		return cfg, nil
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		// Если сертификат повреждён – тоже используем InsecureSkipVerify
		return cfg, nil
	}
	cfg.RootCAs = pool
	cfg.InsecureSkipVerify = false
	return cfg, nil
}

// getEnv возвращает значение переменной окружения или значение по умолчанию
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return defaultValue
}

// ------------------------------------------------------------
// Структуры и кодек
type Word struct {
	Word string `json:"word"`
	Cmd  string `json:"cmd"`
}

type WordCodec struct{}

func (c *WordCodec) Encode(value interface{}) ([]byte, error) {
	if word, ok := value.(Word); ok {
		return json.Marshal(word)
	}
	return nil, fmt.Errorf("illegal type: %T", value)
}

func (c *WordCodec) Decode(data []byte) (interface{}, error) {
	var m Word
	return &m, json.Unmarshal(data, &m)
}

type WordsListCodec struct{}

func (c *WordsListCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *WordsListCodec) Decode(data []byte) (interface{}, error) {
	var m []string
	err := json.Unmarshal(data, &m)
	return m, err
}