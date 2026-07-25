package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"github.com/riferrei/srclient"
)

// ---------------- Модели (структуры) ----------------

// Price представляет цену товара
type Price struct {
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

// Stock представляет остатки на складе
type Stock struct {
	Available int `json:"available"`
	Reserved  int `json:"reserved"`
}

// Image представляет изображение товара
type Image struct {
	URL string `json:"url"`
	Alt string `json:"alt"`
}

// Product – основная структура товара
type Product struct {
	ProductID      string            `json:"product_id"`
	Name           string            `json:"name"`
	Description    string            `json:"description"`
	Price          Price             `json:"price"`
	Category       string            `json:"category"`
	Brand          string            `json:"brand"`
	Stock          Stock             `json:"stock"`
	SKU            string            `json:"sku"`
	Tags           []string          `json:"tags"`
	Images         []Image           `json:"images"`
	Specifications map[string]string `json:"specifications"`
	CreatedAt      string            `json:"created_at"`
	UpdatedAt      string            `json:"updated_at"`
	Index          string            `json:"index"`
	StoreID        string            `json:"store_id"`
}

// ---------------- Конфигурация ----------------

const (
	defaultBroker            = "kafka-1st-1:1092"
	defaultSchemaRegistryURL = "http://schema-registry-1st:8081"
	defaultUsername          = "admin"
	defaultPassword          = "admin-secret"
	defaultCaPath            = "/usr/local/bin/ca.crt"
	defaultTopic             = "product"
	defaultJSONFilePath      = "/home/appuser/data/products_2.json"
)

// ---------------- Основная функция ----------------

func main() {
	// Загружаем .env
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using defaults or system env")
	}

	broker := getEnv("BROKER", defaultBroker)
	schemaRegistryURL := getEnv("SCHEMA_REGISTRY_URL", defaultSchemaRegistryURL)
	username := getEnv("SASL_USERNAME", defaultUsername)
	password := getEnv("SASL_PASSWORD", defaultPassword)
	caPath := getEnv("CA_PATH", defaultCaPath)
	topic := getEnv("TOPIC", defaultTopic)
	jsonFilePath := getEnv("JSON_FILE_PATH", defaultJSONFilePath)

	subject := topic + "-value"

	// Продюсер Kafka
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"security.protocol": "SASL_SSL",
		"sasl.mechanism":    "PLAIN",
		"sasl.username":     username,
		"sasl.password":     password,
		"ssl.ca.location":   caPath,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	// HTTP клиент для Schema Registry
	httpClient, err := httpClientWithCA(caPath)
	if err != nil {
		log.Fatalf("Failed to create HTTP client: %s", err)
	}

	sr := srclient.NewSchemaRegistryClient(schemaRegistryURL, srclient.WithClient(httpClient))
	sr.SetCredentials(username, password)
	sr.CodecCreationEnabled(true)

	fmt.Println("Getting schema...")
	schema, err := sr.GetLatestSchema(subject)
	if err != nil {
		log.Fatalf("Failed to get schema for %s: %s", subject, err)
	}
	fmt.Printf("Schema subject=%s id=%d\n", subject, schema.ID())

	// Читаем JSON-файл
	jsonData, err := os.ReadFile(jsonFilePath)
	if err != nil {
		log.Fatalf("Failed to read file %s: %s", jsonFilePath, err)
	}

	// Парсим: массив продуктов или одиночный продукт
	var products []Product
	if err := json.Unmarshal(jsonData, &products); err != nil {
		var single Product
		if err2 := json.Unmarshal(jsonData, &single); err2 != nil {
			log.Fatalf("Failed to unmarshal JSON: %s (also tried as object: %s)", err, err2)
		}
		products = []Product{single}
		fmt.Println("Detected single product, wrapping into array")
	}

	fmt.Printf("Found %d products to send\n", len(products))

	// Отправляем каждый продукт
	for i, product := range products {
		native := productToNative(product)

		payload, err := encodeAvro(schema, native)
		if err != nil {
			log.Printf("Failed to encode product #%d (ID: %s): %s", i+1, product.ProductID, err)
			continue
		}

		if err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          payload,
		}, nil); err != nil {
			log.Printf("Failed to produce product #%d: %s", i+1, err)
			continue
		}

		fmt.Printf("Product #%d (ID: %s) sent\n", i+1, product.ProductID)
	}

	producer.Flush(15_000)
	fmt.Println("All messages sent")
}

// ---------------- Вспомогательные функции ----------------

// getEnv возвращает значение переменной окружения или значение по умолчанию
func getEnv(key, fallback string) string {
	if val, exists := os.LookupEnv(key); exists {
		return val
	}
	return fallback
}

// productToNative преобразует структуру Product в map[string]interface{} для Avro-кодека
func productToNative(p Product) map[string]interface{} {
	return map[string]interface{}{
		"product_id":  p.ProductID,
		"name":        p.Name,
		"description": p.Description,
		"price": map[string]interface{}{
			"amount":   p.Price.Amount,
			"currency": p.Price.Currency,
		},
		"category": p.Category,
		"brand":    p.Brand,
		"stock": map[string]interface{}{
			"available": p.Stock.Available,
			"reserved":  p.Stock.Reserved,
		},
		"sku": p.SKU,
		"tags": func() []string {
			if p.Tags == nil {
				return []string{}
			}
			return p.Tags
		}(),
		"images": func() []map[string]interface{} {
			imgs := make([]map[string]interface{}, len(p.Images))
			for i, img := range p.Images {
				imgs[i] = map[string]interface{}{
					"url": img.URL,
					"alt": img.Alt,
				}
			}
			return imgs
		}(),
		"specifications": func() map[string]string {
			if p.Specifications == nil {
				return map[string]string{}
			}
			return p.Specifications
		}(),
		"created_at": p.CreatedAt,
		"updated_at": p.UpdatedAt,
		"index":      p.Index,
		"store_id":   p.StoreID,
	}
}

// encodeAvro добавляет магический байт и ID схемы к сериализованным данным Avro
func encodeAvro(schema *srclient.Schema, native interface{}) ([]byte, error) {
	bin, err := schema.Codec().BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 5+len(bin))
	out[0] = 0
	binary.BigEndian.PutUint32(out[1:5], uint32(schema.ID()))
	copy(out[5:], bin)
	return out, nil
}

// httpClientWithCA создаёт HTTP-клиент с указанным CA-сертификатом
func httpClientWithCA(caPath string) (*http.Client, error) {
	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("invalid CA certificate: %s", caPath)
	}
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool},
		},
	}, nil
}
