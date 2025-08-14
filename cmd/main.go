package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Укажи команду: produce, consume или parse")
		return
	}

	InitDB()
	testQuery()

	// Инициализируем локальный кэш и загружаем из БД
	OrderCache = NewCache()

	if err := PopulateCacheFromDB(context.Background(), OrderCache); err != nil {
		log.Fatalf("Не удалось заполнить кэш: %v", err)
	}
	log.Printf("✅ Загружено в кэш: %d заказов", len(OrderCache.store))

	switch os.Args[1] {
	case "produce":
		runProducer()
	case "consume":

		// стартуем consumer в горутине и запускаем HTTP сервер в основном потоке
		ctx := context.Background()
		go runConsumer(ctx)
		http.HandleFunc("/order", orderHandler)

		//подтягиваем html+js
		fs := http.FileServer(http.Dir("./static"))
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				http.NotFound(w, r)
				return
			}
			http.ServeFile(w, r, "./static/index.html")
		})
		http.Handle("/static/", http.StripPrefix("/static/", fs))
		log.Println("Сервер запущен на :8080")
		log.Fatal(http.ListenAndServe(":8080", nil))
	case "parse":
		jsonData := []byte(`{
		"order_uid": "b563feb7b2b84b6test",
			"track_number": "WBILMTESTTRACK",
			"entry": "WBIL",
			"delivery": {
			"name": "Test Testov",
				"phone": "+9720000000",
				"zip": "2639809",
				"city": "Kiryat Mozkin",
				"address": "Ploshad Mira 15",
				"region": "Kraiot",
				"email": "test@gmail.com"
		},
		"payment": {
			"transaction": "b563feb7b2b84b6test",
				"request_id": "",
				"currency": "USD",
				"provider": "wbpay",
				"amount": 1817,
				"payment_dt": 1637907727,
				"bank": "alpha",
				"delivery_cost": 1500,
				"goods_total": 317,
				"custom_fee": 0
		},
		"items": [
	{
	"chrt_id": 9934930,
	"track_number": "WBILMTESTTRACK",
	"price": 453,
	"rid": "ab4219087a764ae0btest",
	"name": "Mascaras",
	"sale": 30,
	"size": "0",
	"total_price": 317,
	"nm_id": 2389212,
	"brand": "Vivienne Sabo",
	"status": 202
	}
	],
	"locale": "en",
	"internal_signature": "",
	"customer_id": "test",
	"delivery_service": "meest",
	"shardkey": "9",
	"sm_id": 99,
	"date_created": "2021-11-26T06:22:19Z",
	"oof_shard": "1"
}`)
		order, err := ParseOrderJSON(jsonData)
		if err != nil {
			log.Fatal("Ошибка парсинга JSON:", err)
		}
		log.Printf("Распарсенный заказ: %+v\n", order)
	default:
		fmt.Println("Неизвестная команда:", os.Args[1])
	}
}

// Order и другие Модели
type Order struct {
	OrderUID          string    `json:"order_uid"`
	TrackNumber       string    `json:"track_number"`
	Entry             string    `json:"entry"`
	Locale            string    `json:"locale"`
	InternalSignature string    `json:"internal_signature"`
	CustomerID        string    `json:"customer_id"`
	DeliveryService   string    `json:"delivery_service"`
	Shardkey          string    `json:"shardkey"`
	SmID              int       `json:"sm_id"`
	DateCreated       time.Time `json:"date_created"`
	OofShard          string    `json:"oof_shard"`
	Delivery          Delivery  `json:"delivery"`
	Payment           Payment   `json:"payment"`
	Items             []Item    `json:"items"`
}

type Delivery struct {
	OrderUID string `json:"order_uid"`
	Name     string `json:"name"`
	Phone    string `json:"phone"`
	Zip      string `json:"zip"`
	City     string `json:"city"`
	Address  string `json:"address"`
	Region   string `json:"region"`
	Email    string `json:"email"`
}

type Payment struct {
	OrderUID     string          `json:"order_uid"`
	Transaction  string          `json:"transaction"`
	RequestID    string          `json:"request_id"`
	Currency     string          `json:"currency"`
	Provider     string          `json:"provider"`
	Amount       decimal.Decimal `json:"amount"`
	PaymentDT    int64           `json:"payment_dt"`
	Bank         string          `json:"bank"`
	DeliveryCost decimal.Decimal `json:"delivery_cost"`
	GoodsTotal   decimal.Decimal `json:"goods_total"`
	CustomFee    decimal.Decimal `json:"custom_fee"`
}

type Item struct {
	ID          int             `json:"id"`
	OrderUID    string          `json:"order_uid"`
	ChrtID      int64           `json:"chrt_id"`
	TrackNumber string          `json:"track_number"`
	Price       decimal.Decimal `json:"price"`
	RID         string          `json:"rid"`
	Name        string          `json:"name"`
	Sale        int             `json:"sale"`
	Size        string          `json:"size"`
	TotalPrice  decimal.Decimal `json:"total_price"`
	NMID        int64           `json:"nm_id"`
	Brand       string          `json:"brand"`
	Status      int             `json:"status"`
}

// Cache Локальный кэш
type Cache struct {
	mu    sync.RWMutex
	store map[string]Order
}

func NewCache() *Cache {
	return &Cache{store: make(map[string]Order)}
}
func (c *Cache) Set(key string, value Order) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[key] = value
}
func (c *Cache) Get(key string) (Order, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.store[key]
	return val, ok
}
func (c *Cache) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]string, 0, len(c.store))
	for k := range c.store {
		keys = append(keys, k)
	}
	return keys
}

var OrderCache = NewCache()

// DB для подключения к БД
var DB *pgxpool.Pool

func InitDB() {
	dsn := "postgres://order_user:pass@localhost:5432/order_service"
	ctx := context.Background()

	var err error
	DB, err = pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("Не удалось подключиться к БД: %v", err)
	}
	if err := DB.Ping(ctx); err != nil {
		log.Fatalf("Ошибка при пинге БД: %v", err)
	}
	fmt.Println("✅ Подключение к БД установлено")
}

func testQuery() {
	var count int
	if err := DB.QueryRow(context.Background(), "SELECT COUNT(*) FROM orders").Scan(&count); err != nil {
		log.Fatal("Ошибка запроса:", err)
	}
	log.Printf("Количество заказов в БД: %d\n", count)
}

// PopulateCacheFromDB Выгрузка всех заказов в кэш
func PopulateCacheFromDB(ctx context.Context, c *Cache) error {
	orders, err := LoadAllOrders(ctx)
	if err != nil {
		return err
	}
	for _, o := range orders {
		if o.OrderUID == "" {
			continue
		}
		c.Set(o.OrderUID, o)
	}
	return nil
}

// LoadAllOrders собирает order + delivery + payment + items
func LoadAllOrders(ctx context.Context) ([]Order, error) {
	rows, err := DB.Query(ctx, `
        SELECT order_uid, track_number, entry, locale, internal_signature,
               customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
        FROM orders
    `)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []Order
	for rows.Next() {
		var o Order
		if err := rows.Scan(
			&o.OrderUID, &o.TrackNumber, &o.Entry, &o.Locale, &o.InternalSignature,
			&o.CustomerID, &o.DeliveryService, &o.Shardkey, &o.SmID, &o.DateCreated, &o.OofShard,
		); err != nil {
			return nil, err
		}

		// загрузить delivery (если есть)
		if err := loadDelivery(ctx, &o); err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		// загрузить payment (если есть)
		if err := loadPayment(ctx, &o); err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		// загрузить items
		if err := loadItems(ctx, &o); err != nil {
			return nil, err
		}

		res = append(res, o)
	}
	return res, nil
}

func loadDelivery(ctx context.Context, o *Order) error {
	return DB.QueryRow(ctx, `
    SELECT order_uid, name, phone, zip, city, address, region, email
    FROM delivery WHERE order_uid = $1
`, o.OrderUID).Scan(
		&o.Delivery.OrderUID, &o.Delivery.Name, &o.Delivery.Phone,
		&o.Delivery.Zip, &o.Delivery.City, &o.Delivery.Address,
		&o.Delivery.Region, &o.Delivery.Email,
	)

}

func loadPayment(ctx context.Context, o *Order) error {
	return DB.QueryRow(ctx, `
    SELECT order_uid, transaction, request_id, currency, provider, amount, payment_dt,
           bank, delivery_cost, goods_total, custom_fee
    FROM payment WHERE order_uid = $1
`, o.OrderUID).Scan(
		&o.Payment.OrderUID, &o.Payment.Transaction, &o.Payment.RequestID,
		&o.Payment.Currency, &o.Payment.Provider, &o.Payment.Amount,
		&o.Payment.PaymentDT, &o.Payment.Bank, &o.Payment.DeliveryCost,
		&o.Payment.GoodsTotal, &o.Payment.CustomFee,
	)
}

func loadItems(ctx context.Context, o *Order) error {
	rows, err := DB.Query(ctx, `
    SELECT order_uid, chrt_id, track_number, price, rid, name, sale, size,
           total_price, nm_id, brand, status
    FROM items WHERE order_uid = $1
`, o.OrderUID)
	if err != nil {
		return err
	}
	defer rows.Close()

	o.Items = nil
	for rows.Next() {
		var it Item
		var ou string // временная переменная для order_uid из БД
		if err := rows.Scan(
			&ou, &it.ChrtID, &it.TrackNumber, &it.Price, &it.RID, &it.Name,
			&it.Sale, &it.Size, &it.TotalPrice, &it.NMID,
			&it.Brand, &it.Status,
		); err != nil {
			return err
		}

		it.OrderUID = ou

		o.Items = append(o.Items, it)
	}
	return nil
}

// SaveOrder Сохранение заказа в БД
func SaveOrder(ctx context.Context, order Order) error {
	if order.OrderUID == "" {
		return fmt.Errorf("empty order_uid")
	}

	tx, err := DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ошибка при начале транзакции: %w", err)
	}
	defer tx.Rollback(ctx)

	order.Delivery.OrderUID = order.OrderUID
	order.Payment.OrderUID = order.OrderUID
	for i := range order.Items {
		order.Items[i].OrderUID = order.OrderUID
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO orders (
			order_uid, track_number, entry, locale,
			internal_signature, customer_id, delivery_service,
			shardkey, sm_id, date_created, oof_shard
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (order_uid) DO NOTHING
	`, order.OrderUID, order.TrackNumber, order.Entry, order.Locale,
		order.InternalSignature, order.CustomerID, order.DeliveryService,
		order.Shardkey, order.SmID, order.DateCreated, order.OofShard)
	if err != nil {
		return fmt.Errorf("ошибка при вставке в orders: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO delivery (order_uid, name, phone, zip, city, address, region, email)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (order_uid) DO NOTHING
	`, order.OrderUID, order.Delivery.Name, order.Delivery.Phone,
		order.Delivery.Zip, order.Delivery.City, order.Delivery.Address,
		order.Delivery.Region, order.Delivery.Email)
	if err != nil {
		return fmt.Errorf("ошибка при вставке в delivery: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO payment (
			order_uid, transaction, request_id, currency,
			provider, amount, payment_dt, bank,
			delivery_cost, goods_total, custom_fee
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (order_uid) DO NOTHING
	`, order.OrderUID, order.Payment.Transaction, order.Payment.RequestID,
		order.Payment.Currency, order.Payment.Provider, order.Payment.Amount,
		order.Payment.PaymentDT, order.Payment.Bank, order.Payment.DeliveryCost,
		order.Payment.GoodsTotal, order.Payment.CustomFee)
	if err != nil {
		return fmt.Errorf("ошибка при вставке в payment: %w", err)
	}

	for _, item := range order.Items {
		_, err = tx.Exec(ctx, `
			INSERT INTO items (
				order_uid, chrt_id, track_number, price,
				rid, name, sale, size,
				total_price, nm_id, brand, status
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			ON CONFLICT (id) DO NOTHING
		`, order.OrderUID, item.ChrtID, item.TrackNumber, item.Price,
			item.RID, item.Name, item.Sale, item.Size, item.TotalPrice,
			item.NMID, item.Brand, item.Status)
		if err != nil {
			return fmt.Errorf("ошибка при вставке в items: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("ошибка при коммите: %w", err)
	}
	return nil
}

// KAFKA Producer|Consumer
func runProducer() {
	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "test-topic",
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	// пример json-значения — можешь менять
	jsonData := []byte(`{
		"order_uid": "2lastTest140825",
			"track_number": "WBILMTESTTRACK",
			"entry": "WBIL",
			"delivery": {
			"name": "Test Testov",
				"phone": "+9720000000",
				"zip": "2639809",
				"city": "Kiryat Mozkin",
				"address": "Ploshad Mira 15",
				"region": "Kraiot",
				"email": "test@gmail.com"
		},
		"payment": {
			"transaction": "b563feb7b2b84b6test",
				"request_id": "",
				"currency": "USD",
				"provider": "wbpay",
				"amount": 1817,
				"payment_dt": 1637907727,
				"bank": "alpha",
				"delivery_cost": 1500,
				"goods_total": 317,
				"custom_fee": 0
		},
		"items": [
	{
	"chrt_id": 9934930,
	"track_number": "WBILMTESTTRACK",
	"price": 453,
	"rid": "ab4219087a764ae0btest",
	"name": "Mascaras",
	"sale": 30,
	"size": "0",
	"total_price": 317,
	"nm_id": 2389212,
	"brand": "Vivienne Sabo",
	"status": 202
	}
	],
	"locale": "en",
	"internal_signature": "",
	"customer_id": "test",
	"delivery_service": "meest",
	"shardkey": "9",
	"sm_id": 99,
	"date_created": "2021-11-26T06:22:19Z",
	"oof_shard": "1"
}`)
	if err := w.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("Key-A"),
		Value: jsonData,
	}); err != nil {
		log.Fatal("Ошибка при отправке:", err)
	}
	log.Println("Сообщение отправлено.")
}

func runConsumer(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       "test-topic",
		GroupID:     "group-new",
		StartOffset: kafka.LastOffset,
		//StartOffset: kafka.FirstOffset, // StartOffset: kafka.LastOffset читаем только новые сообщения
	})
	defer r.Close()

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			log.Println("Ошибка при чтении:", err)
			return // или continue в зависимости от желаемой логики
		}
		log.Printf("Получено сообщение: key=%s value=%s\n", string(msg.Key), string(msg.Value))

		order, err := ParseOrderJSON(msg.Value)
		if err != nil {
			log.Println("Ошибка парсинга JSON:", err)
			continue
		}
		if order.OrderUID == "" {
			log.Println("⚠️ Пропущено сообщение: отсутствует order_uid")
			continue
		}

		if err := SaveOrder(context.Background(), *order); err != nil {
			log.Println("Ошибка сохранения заказа:", err)
			continue
		}

		OrderCache.Set(order.OrderUID, *order)
		log.Printf("🆕 Заказ %s добавлен в кэш", order.OrderUID)

	}
}

// HTTP ручка
func orderHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "Missing id", http.StatusBadRequest)
		return
	}

	if OrderCache == nil {
		http.Error(w, "cache not initialized", http.StatusInternalServerError)
		return
	}
	fmt.Println("Запрос по ID:", id)

	// Печатаем все ключи, которые есть в кэше
	for _, k := range OrderCache.Keys() {
		fmt.Println("📦 Ключ в кэше:", k)
	}

	// Проверяем кэш
	if order, ok := OrderCache.Get(id); ok {
		fmt.Println("✅ Найден заказ:", id)
		_ = json.NewEncoder(w).Encode(order)
		return
	}

	//  Если нет в кэше — ищем в БД
	var order Order
	err := DB.QueryRow(r.Context(), `
        SELECT order_uid, track_number, entry, locale, internal_signature,
               customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
        FROM orders WHERE order_uid = $1
    `, id).Scan(
		&order.OrderUID, &order.TrackNumber, &order.Entry, &order.Locale, &order.InternalSignature,
		&order.CustomerID, &order.DeliveryService, &order.Shardkey, &order.SmID,
		&order.DateCreated, &order.OofShard,
	)
	if err != nil {
		fmt.Println("❌ Заказ не найден:", id)
		http.Error(w, "Order not found", http.StatusNotFound) // для curl
		return
	}

	// Загружаем детали
	_ = loadDelivery(r.Context(), &order)
	_ = loadPayment(r.Context(), &order)
	_ = loadItems(r.Context(), &order)

	//  Добавляем в кэш

	//if OrderCache != nil {
	//	OrderCache.Set(id, order)
	//}

	OrderCache.Set(id, order)
	fmt.Println("🆕 Заказ добавлен в кэш:", id)

	// Отправляем JSON клиенту
	json.NewEncoder(w).Encode(order)
}

// ParseOrderJSON Парсим JSON
func ParseOrderJSON(data []byte) (*Order, error) {
	var order Order
	if err := json.Unmarshal(data, &order); err != nil {
		return nil, err
	}
	return &order, nil
}
