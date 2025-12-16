package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"

	"sync"
	"sync/atomic"
	"time"
)

// Money represents monetary values with currency
type Money struct {
	Amount       string `json:"amount"`
	CurrencyCode string `json:"currency_code"`
}

// PriceSet contains shop and presentment money
type PriceSet struct {
	ShopMoney        Money `json:"shop_money"`
	PresentmentMoney Money `json:"presentment_money"`
}

// Property represents custom line item properties
type Property struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// LineItem represents a product in the order
type LineItem struct {
	ID                  int64      `json:"id"`
	AdminID             string     `json:"admin_graphql_api_id"`
	CurrentQuantity     int        `json:"current_quantity"`
	FulfillableQuantity int        `json:"fulfillable_quantity"`
	ProductID           *int64     `json:"product_id"`
	Title               string     `json:"title"`
	Name                string     `json:"name"`
	VariantTitle        *string    `json:"variant_title"`
	Price               string     `json:"price"`
	Quantity            int        `json:"quantity"`
	Vendor              *string    `json:"vendor"`
	PriceSet            PriceSet   `json:"price_set"`
	Grams               int        `json:"grams"`
	SKU                 *string    `json:"sku"`
	Properties          []Property `json:"properties"`
}

// ShopifyAddress represents billing or shipping address
type ShopifyAddress struct {
	Address1     string   `json:"address1"`
	Address2     *string  `json:"address2"`
	FirstName    string   `json:"first_name"`
	LastName     string   `json:"last_name"`
	Name         string   `json:"name"`
	Phone        *string  `json:"phone"`
	City         string   `json:"city"`
	Zip          string   `json:"zip"`
	Province     string   `json:"province"`
	Country      string   `json:"country"`
	Company      *string  `json:"company"`
	Latitude     *float64 `json:"latitude"`
	Longitude    *float64 `json:"longitude"`
	CountryCode  string   `json:"country_code"`
	ProvinceCode string   `json:"province_code"`
}

type DefaultAddress struct {
	Address1     string  `json:"address1"`
	Address2     *string `json:"address2"`
	FirstName    string  `json:"first_name"`
	LastName     string  `json:"last_name"`
	Name         string  `json:"name"`
	Phone        *string `json:"phone"`
	City         string  `json:"city"`
	Zip          string  `json:"zip"`
	Province     string  `json:"province"`
	Country      string  `json:"country"`
	Company      *string `json:"company"`
	CountryCode  string  `json:"country_code"`
	ProvinceCode string  `json:"province_code"`
}

// ShopifyCustomer represents customer information
type ShopifyCustomer struct {
	ID             int64          `json:"id"`
	AdminID        string         `json:"admin_graphql_api_id"`
	Email          string         `json:"email"`
	Phone          *string        `json:"phone"`
	FirstName      string         `json:"first_name"`
	LastName       string         `json:"last_name"`
	DefaultAddress DefaultAddress `json:"default_address"`
}

// ShippingLine represents shipping information
type ShippingLine struct {
	ID                 int64    `json:"id"`
	Code               string   `json:"code"`
	Price              string   `json:"price"`
	PriceSet           PriceSet `json:"price_set"`
	DiscountedPrice    string   `json:"discounted_price"`
	DiscountedPriceSet PriceSet `json:"discounted_price_set"`
	Source             string   `json:"source"`
	Title              string   `json:"title"`
}

// ShopifyOrder represents the main order webhook payload
type ShopifyOrder struct {
	ID                     int64           `json:"id"`
	AdminGraphqlAPIID      string          `json:"admin_graphql_api_id"`
	ContactEmail           string          `json:"contact_email"`
	CreatedAt              string          `json:"created_at"`
	Currency               string          `json:"currency"`
	CurrentTotalPrice      string          `json:"current_total_price"`
	CurrentTotalPriceSet   PriceSet        `json:"current_total_price_set"`
	Name                   string          `json:"name"`
	OrderNumber            int64           `json:"order_number"`
	BillingAddress         ShopifyAddress  `json:"billing_address"`
	Customer               ShopifyCustomer `json:"customer"`
	ShippingAddress        ShopifyAddress  `json:"shipping_address"`
	TotalLineItemsPrice    string          `json:"total_line_items_price"`
	TotalLineItemsPriceSet PriceSet        `json:"total_line_items_price_set"`
	SubtotalPrice          string          `json:"subtotal_price"`
	SubtotalPriceSet       PriceSet        `json:"subtotal_price_set"`
	TotalWeight            int             `json:"total_weight"`
	LineItems              []LineItem      `json:"line_items"`
	ShippingLines          []ShippingLine  `json:"shipping_lines"`
	FinancialStatus        string          `json:"financial_status"`
	FulfillmentStatus      interface{}     `json:"fulfillment_status"`
}

var (
	printReadyFiles = []string{
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-1-22x5.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-2-22x10.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-3-22x20.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-4-22x30.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-5-22x40.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-6-22x50.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-7-22x60.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-8-22x70.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-9-22x80.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-10-22x90.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-11-22x100.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-12-22x110.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-13-22x120.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-14-22x130.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-15-22x140.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-16-22x150.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-17-22x160.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-18-22x170.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-19-22x180.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-20-22x190.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-21-22x200.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-22-22x250.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-23-22x300.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-24-22x400.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-25-22x500.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-26-22x600.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-27-22x750.png",
		"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-28-22x1000.png",
	}
	variants = []string{
		"22x5",
		"22x10",
		"22x20",
		"22x30",
		"22x40",
		"22x50",
		"22x60",
		"22x70",
		"22x80",
		"22x90",
		"22x100",
		"22x110",
		"22x120",
		"22x130",
		"22x140",
		"22x150",
		"22x160",
		"22x170",
		"22x180",
		"22x190",
		"22x200",
		"22x250",
		"22x300",
		"22x400",
		"22x500",
		"22x600",
		"22x750",
		"22x1000",
	}
	firstNames = []string{"John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Lisa", "James", "Mary"}
	lastNames  = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}
	cityData   = []struct {
		city      string
		state     string
		stateName string
		zipCodes  string
		streets   string
		latitude  float64
		longitude float64
	}{
		{
			"New York", "NY", "New York",
			"10003",
			"20 Cooper Square",
			32.7767, -96.7970,
		},
		{
			"Brooklyn", "NY", "New York",
			"11201",
			"6 Metrotech Center",
			32.7767, -96.7970,
		},
		{
			"Houston", "TX", "Texas",
			"77001",
			"1234 Main Street",
			32.7767, -96.7970,
		},
		{
			"Glen Allen", "VA", "Virginia",
			"23060",
			"10260 W Broad St",
			38.7667, -77.2170,
		},
	}
)

type Stats struct {
	TotalRequests   int64
	SuccessRequests int64
	FailedRequests  int64
	TotalDuration   int64 // in milliseconds
}

func generateOrder(orderID int64, timestamp time.Time) ShopifyOrder {
	firstNameIdx := rand.Intn(len(firstNames))
	lastNameIdx := rand.Intn(len(lastNames))
	cityIdx := rand.Intn(len(cityData))

	firstName := firstNames[firstNameIdx]
	lastName := lastNames[lastNameIdx]
	email := fmt.Sprintf("%s.%s%d@example.com", firstName, lastName, orderID)

	price := fmt.Sprintf("%.2f", 10.0+rand.Float64()*90.0)
	shippingPrice := "4.90"
	subtotal := price
	totalPrice := fmt.Sprintf("%.2f", mustParseFloat(price)+mustParseFloat(shippingPrice))

	productID := int64(8779236999337)
	//randItem := rand.Intn(len(variants))
	//variantTitle := variants[randItem]

	vendor := "DTFsheet and custom shirts"

	// random the number of line items
	numLineItems := rand.Intn(5) + 1

	lineItems := make([]LineItem, numLineItems)
	for i := 0; i < numLineItems; i++ {
		// random quantity
		quantity := rand.Intn(10) + 1
		randLineItem := rand.Intn(len(printReadyFiles))
		lineItems[i] = LineItem{
			ID:                  15573094760617 + orderID + int64(i),
			AdminID:             fmt.Sprintf("gid://shopify/LineItem/%d", 15573094760617+orderID+int64(i)),
			CurrentQuantity:     1,
			FulfillableQuantity: 1,
			ProductID:           &productID,
			Title:               "DTF GANG SHEET BUILDER",
			Name:                fmt.Sprintf("DTF Gangsheet %s", variants[randLineItem]),
			VariantTitle:        &variants[randLineItem],
			Price:               price,
			Quantity:            quantity,
			Vendor:              &vendor,
			PriceSet: PriceSet{
				ShopMoney:        Money{Amount: price, CurrencyCode: "USD"},
				PresentmentMoney: Money{Amount: price, CurrencyCode: "USD"},
			},
			Grams: 0,
			Properties: []Property{
				{Name: "Preview", Value: "https://app.dripappsserver.com/preview/fcc2f5fe-551c-40d0-bcd6-562e4ec6575d.png"},
				{Name: "Edit", Value: "https://app.dripappsserver.com/builder/edit?design_id=fcc2f5fe-551c-40d0-bcd6-562e4ec6575d"},
				{Name: "_Admin Edit", Value: "https://app.dripappsserver.com/builder/edit?design_id=fcc2f5fe-551c-40d0-bcd6-562e4ec6575d&token=3NFkVDViB0WAAOlARn7W"},
				{Name: "_Print Ready File", Value: printReadyFiles[randLineItem]},
				{Name: "_Actual Height", Value: "3.76 in"},
				{Name: "Additional Note", Value: fmt.Sprintf("Test Order %d", orderID)},
				{Name: "Background Removal", Value: "No"},
			},
		}
	}

	// Get city data
	city := cityData[cityIdx]

	orderName := fmt.Sprintf("#%f-%s", rand.Float64(), firstNames[numLineItems-1])

	return ShopifyOrder{
		ID:                6574664908969 + orderID, // fmt.Sprintf("657466490896%d", orderID),
		AdminGraphqlAPIID: fmt.Sprintf("gid://shopify/Order/%d", 6574664908969+orderID),
		ContactEmail:      email,
		CreatedAt:         timestamp.Format(time.RFC3339),
		Currency:          "USD",
		CurrentTotalPrice: totalPrice,
		CurrentTotalPriceSet: PriceSet{
			ShopMoney:        Money{Amount: totalPrice, CurrencyCode: "USD"},
			PresentmentMoney: Money{Amount: totalPrice, CurrencyCode: "USD"},
		},
		Name:        orderName,
		OrderNumber: orderID,
		BillingAddress: ShopifyAddress{
			FirstName:    firstName,
			LastName:     lastName,
			Name:         fmt.Sprintf("%s %s", firstName, lastName),
			Address1:     city.streets,
			City:         city.city,
			Zip:          city.zipCodes,
			Province:     city.stateName,
			Country:      "United States",
			CountryCode:  "US",
			ProvinceCode: city.state,
			Latitude:     ptrFloat64(city.latitude + (rand.Float64()-0.5)*0.1),
			Longitude:    ptrFloat64(city.longitude + (rand.Float64()-0.5)*0.1),
		},
		Customer: ShopifyCustomer{
			ID:        8909317734569 + orderID,
			Email:     email,
			FirstName: firstName,
			LastName:  lastName,
			DefaultAddress: DefaultAddress{
				FirstName:    firstName,
				LastName:     lastName,
				Name:         fmt.Sprintf("%s %s", firstName, lastName),
				Address1:     city.streets,
				City:         city.city,
				Zip:          city.zipCodes,
				Province:     city.stateName,
				Country:      "United States",
				CountryCode:  "US",
				ProvinceCode: city.state,
			},
		},
		ShippingAddress: ShopifyAddress{
			FirstName:    firstName,
			LastName:     lastName,
			Name:         fmt.Sprintf("%s %s", firstName, lastName),
			Address1:     city.streets,
			City:         city.city,
			Zip:          city.zipCodes,
			Province:     city.stateName,
			Country:      "United States",
			CountryCode:  "US",
			ProvinceCode: city.state,
			Latitude:     ptrFloat64(city.latitude + (rand.Float64()-0.5)*0.1),
			Longitude:    ptrFloat64(city.longitude + (rand.Float64()-0.5)*0.1),
		},
		TotalLineItemsPrice: subtotal,
		TotalLineItemsPriceSet: PriceSet{
			ShopMoney:        Money{Amount: subtotal, CurrencyCode: "USD"},
			PresentmentMoney: Money{Amount: subtotal, CurrencyCode: "USD"},
		},
		SubtotalPrice: subtotal,
		SubtotalPriceSet: PriceSet{
			ShopMoney:        Money{Amount: subtotal, CurrencyCode: "USD"},
			PresentmentMoney: Money{Amount: subtotal, CurrencyCode: "USD"},
		},
		TotalWeight:       0,
		FinancialStatus:   "paid",
		FulfillmentStatus: nil,
		LineItems:         lineItems,
		ShippingLines: []ShippingLine{
			{
				ID:    5468266823849 + orderID,
				Code:  "Economy",
				Price: shippingPrice,
				PriceSet: PriceSet{
					ShopMoney:        Money{Amount: shippingPrice, CurrencyCode: "USD"},
					PresentmentMoney: Money{Amount: shippingPrice, CurrencyCode: "USD"},
				},
				DiscountedPrice: shippingPrice,
				DiscountedPriceSet: PriceSet{
					ShopMoney:        Money{Amount: shippingPrice, CurrencyCode: "USD"},
					PresentmentMoney: Money{Amount: shippingPrice, CurrencyCode: "USD"},
				},
				Source: "shopify",
				Title:  "Economy",
			},
		},
	}
}

func ptrFloat64(f float64) *float64 {
	return &f
}

func mustParseFloat(s string) float64 {
	var f float64
	fmt.Sscanf(s, "%f", &f)
	return f
}

func sendWebhook(ctx context.Context, client *http.Client, url string, order ShopifyOrder, stats *Stats) error {
	payload, err := json.Marshal(order)
	if err != nil {
		return err
	}

	//fmt.Println("Send")
	//var out bytes.Buffer
	//json.Indent(&out, payload, "", "\t")
	//out.WriteTo(os.Stdout)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Shopify-Topic", "orders/create")
	req.Header.Set("X-Shopify-Hmac-SHA256", "test-signature")
	req.Header.Set("X-Shopify-Shop-Domain", "dtfgangsheet.myshopify.com")

	start := time.Now()
	resp, err := client.Do(req)
	duration := time.Since(start).Milliseconds()

	atomic.AddInt64(&stats.TotalDuration, duration)

	if err != nil {
		atomic.AddInt64(&stats.FailedRequests, 1)
		return err
	}
	defer resp.Body.Close()

	// Drain body so HTTP/2 stream ends cleanly instead of RST_STREAM spam
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		atomic.AddInt64(&stats.SuccessRequests, 1)
	} else {
		atomic.AddInt64(&stats.FailedRequests, 1)
		return fmt.Errorf("server returned status: %d", resp.StatusCode)
	}

	return nil
}

// const url = "https://dtf-api-dedicated.daovudat.site/webhooks/test/orders/create"
//const url = "http://localhost:8000/webhooks/test/orders/create"

const url = "https://dtf-api.daovudat.site/webhooks/test/orders/create"

//const url = "https://dtf-api4gb-asia.daovudat.site/webhooks/test/orders/create"

func main() {
	webhookURL := flag.String("url", url, "Webhook endpoint URL")
	totalOrders := flag.Int("total", 80000, "Total number of orders to send")
	ratePerMinute := flag.Int("rate", 2000, "Number of requests per minute")
	_ = flag.Int("duration", 60, "Duration in minutes (0 = send all at configured rate)")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent workers")
	flag.Parse()

	log.Printf("Starting webhook load test...")
	log.Printf("Target URL: %s", *webhookURL)
	log.Printf("Total Orders: %d", *totalOrders)
	log.Printf("Rate: %d req/min", *ratePerMinute)
	log.Printf("Concurrency: %d", *concurrency)

	rand.Seed(time.Now().UnixNano())

	stats := &Stats{}
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     100,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true, // keep HTTP/2, but better managed
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   3 * time.Minute,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	orderChan := make(chan int64, *concurrency*2)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for orderID := range orderChan {
				seconds := rand.Int63n(60 * 24 * 60 * 60)
				timestamp := time.Now().Add(-time.Duration(seconds) * time.Second)
				order := generateOrder(orderID, timestamp)

				err := sendWebhook(ctx, client, *webhookURL, order, stats)
				atomic.AddInt64(&stats.TotalRequests, 1)

				if err != nil {
					log.Printf("Worker %d: Error sending order %d: %v", workerID, orderID, err)
				}
			}
		}(i)
	}

	// Generate orders at specified rate
	startTime := time.Now()
	ticker := time.NewTicker(time.Minute / time.Duration(*ratePerMinute))
	defer ticker.Stop()

	go func() {
		for i := int64(1); i <= int64(*totalOrders); i++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				orderChan <- i
			}
		}
		close(orderChan)
	}()

	// Stats reporter
	statsTicker := time.NewTicker(10 * time.Second)
	defer statsTicker.Stop()

	go func() {
		for range statsTicker.C {
			total := atomic.LoadInt64(&stats.TotalRequests)
			success := atomic.LoadInt64(&stats.SuccessRequests)
			failed := atomic.LoadInt64(&stats.FailedRequests)
			totalDur := atomic.LoadInt64(&stats.TotalDuration)

			avgLatency := int64(0)
			if total > 0 {
				avgLatency = totalDur / total
			}

			elapsed := time.Since(startTime)
			rps := float64(total) / elapsed.Seconds()

			log.Printf("Stats: Total=%d, Success=%d, Failed=%d, RPS=%.2f, AvgLatency=%dms",
				total, success, failed, rps, avgLatency)
		}
	}()

	wg.Wait()

	// Final stats
	elapsed := time.Since(startTime)
	total := atomic.LoadInt64(&stats.TotalRequests)
	success := atomic.LoadInt64(&stats.SuccessRequests)
	failed := atomic.LoadInt64(&stats.FailedRequests)
	totalDur := atomic.LoadInt64(&stats.TotalDuration)

	avgLatency := int64(0)
	if total > 0 {
		avgLatency = totalDur / total
	}

	log.Printf("\n=== Final Results ===")
	log.Printf("Total Time: %v", elapsed)
	log.Printf("Total Requests: %d", total)
	log.Printf("Successful: %d", success)
	log.Printf("Failed: %d", failed)
	log.Printf("Success Rate: %.2f%%", float64(success)/float64(total)*100)
	log.Printf("Average RPS: %.2f", float64(total)/elapsed.Seconds())
	log.Printf("Average Latency: %dms", avgLatency)
}
