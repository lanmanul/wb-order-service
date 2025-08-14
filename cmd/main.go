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
