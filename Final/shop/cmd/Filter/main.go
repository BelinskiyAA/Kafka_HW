package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
	"github.com/linkedin/goavro/v2"
	"github.com/lovoo/goka"
	"github.com/riferrei/srclient"
)

// ---------- Переменные окружения (с дефолтами) ----------
const (
	defaultBroker             = "kafka-1st-1:1092"
	defaultSchemaRegistryURL  = "http://schema-registry-1st:8081"
	defaultTopicProduct       = "product"
	defaultTopicFilterProduct = "filter-product"
	defaultTopicBlock         = "block-product"
	defaultUsername           = "admin"
	defaultPassword           = "admin-secret"
	defaultCaPath             = "/usr/local/bin/ca.crt"
)

var (
	brokers []string
	srURL   string

	topicProduct       goka.Stream
	topicFilterProduct goka.Stream
	topicBlock         goka.Stream

	groupBlock      goka.Group
	groupBlockTable goka.Table
)

func init() {
	// Загружаем .env (если есть)
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using defaults or system env")
	}

	// Читаем переменные
	broker := getEnv("BROKER", defaultBroker)
	brokers = []string{broker}
	srURL = getEnv("SCHEMA_REGISTRY_URL", defaultSchemaRegistryURL)
	topicProduct = goka.Stream(getEnv("TOPIC_PRODUCT", defaultTopicProduct))
	topicFilterProduct = goka.Stream(getEnv("TOPIC_FILTER_PRODUCT", defaultTopicFilterProduct))
	topicBlock = goka.Stream(getEnv("TOPIC_BLOCK", defaultTopicBlock))
	groupBlock = goka.Group(getEnv("GROUP_BLOCK", "block-product-group"))
	groupBlockTable = goka.GroupTable(groupBlock)

	// Инициализация Avro-кодеков
	initAvroCodecs()
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return defaultValue
}

// ---------- Кодек для Product (читает Avro) ----------
type ProductAvroCodec struct {
	codec *goavro.Codec
}

func NewProductAvroCodec() (*ProductAvroCodec, error) {
	srClient := srclient.CreateSchemaRegistryClient(srURL)
	schema, err := srClient.GetLatestSchema("product-value")
	if err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(schema.Schema())
	if err != nil {
		return nil, err
	}
	return &ProductAvroCodec{codec: codec}, nil
}

func (c *ProductAvroCodec) Encode(value interface{}) ([]byte, error) {
	return nil, fmt.Errorf("ProductAvroCodec is consumer-only")
}

func (c *ProductAvroCodec) Decode(data []byte) (interface{}, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("data too short")
	}
	if data[0] != 0x00 {
		return nil, fmt.Errorf("invalid magic byte")
	}
	native, _, err := c.codec.NativeFromBinary(data[5:])
	if err != nil {
		return nil, err
	}
	m, ok := native.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected map, got %T", native)
	}
	return m, nil
}

// ---------- Кодек для ProductRow (записывает Avro) ----------
type ProductRowAvroCodec struct {
	codec    *goavro.Codec
	schemaID int
}

func NewProductRowAvroCodec() (*ProductRowAvroCodec, error) {
	srClient := srclient.CreateSchemaRegistryClient(srURL)
	schema, err := srClient.GetLatestSchema("filter-product-value")
	if err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(schema.Schema())
	if err != nil {
		return nil, err
	}
	return &ProductRowAvroCodec{
		codec:    codec,
		schemaID: schema.ID(),
	}, nil
}

func (c *ProductRowAvroCodec) Encode(value interface{}) ([]byte, error) {
	m, ok := value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected map, got %T", value)
	}
	bin, err := c.codec.BinaryFromNative(nil, m)
	if err != nil {
		return nil, err
	}
	schemaIDBytes := make([]byte, 4)
	id := c.schemaID
	schemaIDBytes[0] = byte(id >> 24)
	schemaIDBytes[1] = byte(id >> 16)
	schemaIDBytes[2] = byte(id >> 8)
	schemaIDBytes[3] = byte(id)
	result := append([]byte{0x00}, schemaIDBytes...)
	result = append(result, bin...)
	return result, nil
}

func (c *ProductRowAvroCodec) Decode(data []byte) (interface{}, error) {
	return nil, fmt.Errorf("ProductRowAvroCodec is producer-only")
}

// ---------- Инициализация кодеков ----------
var productAvroCodec *ProductAvroCodec
var productRowAvroCodec *ProductRowAvroCodec

func initAvroCodecs() {
	var err error
	productAvroCodec, err = NewProductAvroCodec()
	if err != nil {
		log.Fatalf("Failed to create ProductAvroCodec: %v", err)
	}
	productRowAvroCodec, err = NewProductRowAvroCodec()
	if err != nil {
		log.Fatalf("Failed to create ProductRowAvroCodec: %v", err)
	}
}

// ---------- Основная функция ----------
func main() {
	if err := configureGoka(); err != nil {
		log.Fatalf("Failed to configure Goka: %v", err)
	}
	go productProcessor()
	go blockProcessor()
	select {}
}

// ---------- Процессор фильтрации товаров ----------
func productProcessor() {
	log.Println("Start product processor")

	processFunc := func(ctx goka.Context, msg interface{}) {
		productMap, ok := msg.(map[string]interface{})
		if !ok {
			log.Printf("illegal type: %T", msg)
			return
		}

		productID, _ := productMap["product_id"].(string)
		name, _ := productMap["name"].(string)

		var blockedProducts []string
		if val := ctx.Lookup(groupBlockTable, "censor"); val != nil {
			blockedProducts = val.([]string)
		}

		blocked := false
		for _, bp := range blockedProducts {
			if bp == productID || bp == name {
				blocked = true
				break
			}
		}

		if blocked {
			log.Printf("Product %s (%s) is blocked, skipping", productID, name)
			return
		}

		rowMap := make(map[string]interface{})
		rowMap["product_id"] = productID
		rowMap["name"] = name
		rowMap["description"] = getString(productMap, "description")

		if price, ok := productMap["price"].(map[string]interface{}); ok {
			rowMap["price_amount"] = price["amount"]
			rowMap["price_currency"] = price["currency"]
		} else {
			rowMap["price_amount"] = 0.0
			rowMap["price_currency"] = "RUB"
		}
		rowMap["category"] = getString(productMap, "category")
		rowMap["brand"] = getString(productMap, "brand")

		if stock, ok := productMap["stock"].(map[string]interface{}); ok {
			rowMap["stock_available"] = stock["available"]
			rowMap["stock_reserved"] = stock["reserved"]
		} else {
			rowMap["stock_available"] = 0
			rowMap["stock_reserved"] = 0
		}
		rowMap["sku"] = getString(productMap, "sku")
		rowMap["tags"] = toJSONString(productMap["tags"])
		rowMap["images"] = toJSONString(productMap["images"])
		rowMap["specifications"] = toJSONString(productMap["specifications"])
		rowMap["created_at"] = getString(productMap, "created_at")
		rowMap["updated_at"] = getString(productMap, "updated_at")
		rowMap["index"] = getString(productMap, "index")
		rowMap["store_id"] = getString(productMap, "store_id")

		log.Printf("Product %s (%s) is allowed, forwarding", productID, name)
		ctx.Emit(topicFilterProduct, productID, rowMap)
	}

	g := goka.DefineGroup(goka.Group("product-processor-group"),
		goka.Input(topicProduct, productAvroCodec, processFunc),
		goka.Lookup(groupBlockTable, new(WordsListCodec)),
		goka.Output(topicFilterProduct, productRowAvroCodec),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Stop()

	if err = p.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// Вспомогательные функции
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func toJSONString(v interface{}) string {
	if v == nil {
		return "[]"
	}
	b, err := json.Marshal(v)
	if err != nil {
		log.Printf("Failed to marshal to JSON: %v", err)
		return "[]"
	}
	return string(b)
}

// ---------- Кодек для списка строк ----------
type WordsListCodec struct{}

func (c *WordsListCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *WordsListCodec) Decode(data []byte) (interface{}, error) {
	var m []string
	err := json.Unmarshal(data, &m)
	return m, err
}

// ---------- Настройка Goka (SASL_SSL) ----------
func configureGoka() error {
	cfg := goka.DefaultConfig()
	cfg.Version = sarama.V2_8_0_0

	username := getEnv("SASL_USERNAME", "admin")
	password := getEnv("SASL_PASSWORD", "admin-secret")
	caPath := getEnv("CA_PATH", "/usr/local/bin/ca.crt")

	tlsConfig, err := loadTLSConfig(caPath)
	if err != nil {
		return fmt.Errorf("failed to load TLS config: %w", err)
	}
	cfg.Net.TLS.Enable = true
	cfg.Net.TLS.Config = tlsConfig

	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	cfg.Net.SASL.User = username
	cfg.Net.SASL.Password = password

	cfg.Producer.Return.Successes = true
	cfg.Consumer.Return.Errors = true

	goka.ReplaceGlobalConfig(cfg)
	return nil
}

func loadTLSConfig(caPath string) (*tls.Config, error) {
	cfg := &tls.Config{
		InsecureSkipVerify: true,
	}
	if caPath == "" {
		return cfg, nil
	}
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return cfg, nil
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return cfg, nil
	}
	cfg.RootCAs = pool
	cfg.InsecureSkipVerify = false
	return cfg, nil
}

// Word – команда для добавления/удаления товара из чёрного списка
type Word struct {
	Word string `json:"word"`
	Cmd  string `json:"cmd"` // "add" или "rm"
}

// WordCodec – кодек для Word
type WordCodec struct{}

func (c *WordCodec) Encode(value interface{}) ([]byte, error) {
	if w, ok := value.(Word); ok {
		return json.Marshal(w)
	}
	return nil, fmt.Errorf("illegal type: %T", value)
}

func (c *WordCodec) Decode(data []byte) (interface{}, error) {
	var w Word
	return &w, json.Unmarshal(data, &w)
}

func blockProcessor() {
	log.Println("Start block processor")

	processFunc := func(ctx goka.Context, msg interface{}) {
		word, ok := msg.(*Word)
		if !ok {
			log.Printf("illegal type: %T", msg)
			return
		}

		// Получаем текущее значение для ключа (ключ = "censor")
		var words []string
		if val := ctx.Value(); val != nil {
			words = val.([]string)
		}

		// Модифицируем список
		switch word.Cmd {
		case "add":
			found := false
			for _, w := range words {
				if w == word.Word {
					found = true
					break
				}
			}
			if !found {
				words = append(words, word.Word)
			}
		case "rm":
			newWords := []string{}
			for _, w := range words {
				if w != word.Word {
					newWords = append(newWords, w)
				}
			}
			words = newWords
		default:
			log.Printf("unknown command: %s", word.Cmd)
			return
		}

		// Сохраняем обновлённый список
		ctx.SetValue(words)
	}

	g := goka.DefineGroup(groupBlock,
		goka.Input(topicBlock, new(WordCodec), processFunc),
		goka.Persist(new(WordsListCodec)),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Stop()

	if err = p.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

