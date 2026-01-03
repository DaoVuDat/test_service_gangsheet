package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"runtime/debug"
	"sync"
	"time"
)

const PollInterval = 1 * time.Second

func main() {

	var wg sync.WaitGroup
	// 5 workers pool
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go worker(i, &wg)
	}

	wg.Wait()
}

var processedImagesMap = map[string]string{

	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-1-22x5.png":     "./pdf/tmp-out-ABC-1-22x5.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-2-22x10.png":    "./pdf/tmp-out-ABC-2-22x10.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-3-22x20.png":    "./pdf/tmp-out-ABC-3-22x20.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-4-22x30.png":    "./pdf/tmp-out-ABC-4-22x30.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-5-22x40.png":    "./pdf/tmp-out-ABC-5-22x40.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-6-22x50.png":    "./pdf/tmp-out-ABC-6-22x50.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-7-22x60.png":    "./pdf/tmp-out-ABC-7-22x60.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-8-22x70.png":    "./pdf/tmp-out-ABC-8-22x70.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-9-22x80.png":    "./pdf/tmp-out-ABC-9-22x80.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-10-22x90.png":   "./pdf/tmp-out-ABC-10-22x90.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-11-22x100.png":  "./pdf/tmp-out-ABC-11-22x100.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-12-22x110.png":  "./pdf/tmp-out-ABC-12-22x110.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-13-22x120.png":  "./pdf/tmp-out-ABC-13-22x120.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-14-22x130.png":  "./pdf/tmp-out-ABC-14-22x130.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-15-22x140.png":  "./pdf/tmp-out-ABC-15-22x140.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-16-22x150.png":  "./pdf/tmp-out-ABC-16-22x150.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-17-22x160.png":  "./pdf/tmp-out-ABC-17-22x160.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-18-22x170.png":  "./pdf/tmp-out-ABC-18-22x170.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-19-22x180.png":  "./pdf/tmp-out-ABC-19-22x180.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-20-22x190.png":  "./pdf/tmp-out-ABC-20-22x190.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-21-22x200.png":  "./pdf/tmp-out-ABC-21-22x200.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-22-22x250.png":  "./pdf/tmp-out-ABC-22-22x250.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-23-22x300.png":  "./pdf/tmp-out-ABC-23-22x300.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-24-22x400.png":  "./pdf/tmp-out-ABC-24-22x400.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-25-22x500.png":  "./pdf/tmp-out-ABC-25-22x500.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-26-22x600.png":  "./pdf/tmp-out-ABC-26-22x600.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-27-22x750.png":  "./pdf/tmp-out-ABC-27-22x750.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-28-22x1000.png": "./pdf/tmp-out-ABC-28-22x1000.pdf",
}

var processedImagesMapUrl = map[string]string{

	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-1-22x5.png":     "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-1-22x5.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-2-22x10.png":    "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-2-22x10.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-3-22x20.png":    "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-3-22x20.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-4-22x30.png":    "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-4-22x30.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-5-22x40.png":    "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-5-22x40.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-6-22x50.png":    "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-6-22x50.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-7-22x60.png":    "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-7-22x60.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-8-22x70.png":    "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-8-22x70.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-9-22x80.png":    "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-9-22x80.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-10-22x90.png":   "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-10-22x90.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-11-22x100.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-11-22x100.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-12-22x110.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-12-22x110.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-13-22x120.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-13-22x120.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-14-22x130.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-14-22x130.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-15-22x140.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-15-22x140.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-16-22x150.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-16-22x150.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-17-22x160.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-17-22x160.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-18-22x170.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-18-22x170.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-19-22x180.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-19-22x180.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-20-22x190.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-20-22x190.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-21-22x200.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-21-22x200.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-22-22x250.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-22-22x250.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-23-22x300.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-23-22x300.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-24-22x400.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-24-22x400.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-25-22x500.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-25-22x500.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-26-22x600.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-26-22x600.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-27-22x750.png":  "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-27-22x750.pdf",
	"https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-img-ABC-28-22x1000.png": "https://pub-ac878ecfb32d4fabac12c91472c4714a.r2.dev/samples/tmp-out-ABC-28-22x1000.pdf",
}

type Response[T any] struct {
	Data      T         `json:"data,omitempty" doc:"Response data"`
	Message   string    `json:"message,omitempty" doc:"Human-readable message"`
	Timestamp time.Time `json:"timestamp" doc:"Response timestamp"`
}

type LoginRequest struct {
	UserName string `json:"username"`
	Password string `json:"password"`
}

type PresignedRequest struct {
	Key string `json:"key"`
}

type ProcessOrderProductRequest struct {
	FinalImgUrl string `json:"final_img_url"`
}

type AuthResponse struct {
	AccessToken string `json:"access_token"`
	UserName    string `json:"user_name"`
	UserId      int    `json:"user_id"`
	Role        string `json:"role"`
	Name        string `json:"name"`
}

type OrderProductsResponse struct {
	ID             int64     `json:"id"`
	OrderID        int64     `json:"order_id"`
	FulfillmentID  string    `json:"fulfillment_id"`
	ProductID      int64     `json:"product_id"`
	Sku            string    `json:"sku"`
	CustomerImgUrl string    `json:"customer_img_url"`
	FinalImgUrl    string    `json:"final_img_url"`
	Processed      bool      `json:"processed"`
	CreatedAt      time.Time `json:"created_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
	Quantity       int32     `json:"quantity"`
}

type PresignedResponse struct {
	URL         string `json:"upload_url"`
	DownloadURL string `json:"download_url"`
	ExpiresAt   string `json:"expires_at"`
	Key         string `json:"key"`
}

//const url = "https://dtf-api-dedicated.daovudat.site"

//const url = "https://dtf-api.daovudat.site"

//const url = "http://localhost:8000"

const url = "https://dtf-api-chicago.daovudat.site"

const numberOfOrderToProcess = 500

func worker(idx int, wg *sync.WaitGroup) {

	backoff := PollInterval
	log.Printf("Worker %d started", idx)

	// 0. Login
	urlLogin := fmt.Sprintf("%s/auth/login", url)
	choice := rand.Intn(3)
	var logReq LoginRequest
	switch choice {
	case 0:
		logReq = LoginRequest{
			UserName: "admin",
			Password: "admin",
		}
	case 1:
		logReq = LoginRequest{
			UserName: "designer1",
			Password: "designer",
		}
	case 2:
		logReq = LoginRequest{
			UserName: "designer2",
			Password: "designer",
		}
	}
	payload, _ := json.Marshal(logReq)

	req, err := http.NewRequestWithContext(
		context.Background(),
		"POST",
		urlLogin,
		bytes.NewBuffer(payload))
	if err != nil {
		log.Fatalf("worker-%d: failed to create login request: %v", idx, err)
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("worker-%d: failed to login: %v", idx, err)
	}

	if resp.StatusCode != 200 {
		log.Fatalf("worker-%d: login failed", idx)
	}
	var authResp Response[AuthResponse]
	if err := json.NewDecoder(resp.Body).Decode(&authResp); err != nil {
		log.Fatalf("worker-%d: failed to decode login response: %v", idx, err)
	}
	accessToken := authResp.Data.AccessToken

	numProcessedOrder := 0

	for {

		if numProcessedOrder == numberOfOrderToProcess {
			break
		}

		processed := func() bool {
			// recovering from panic
			defer func() {
				if r := recover(); r != nil {
					log.Printf("worker-%d: panic recovered: %v\n%s",
						idx, r, debug.Stack())
					// Worker continues to next tick
				}
			}()

			// 1. Get next order
			getNextOrderUrl := fmt.Sprintf("%s/orders/next", url)

			reqNextOrder, err := http.NewRequestWithContext(
				context.Background(),
				"GET",
				getNextOrderUrl,
				nil)
			if err != nil {
				log.Fatalf("worker-%d: failed to create next order request: %v", idx, err)
				return true
			}

			reqNextOrder.Header.Set("Authorization", "Bearer "+accessToken)
			respNextOrder, err := http.DefaultClient.Do(reqNextOrder)
			if err != nil {
				log.Printf("worker-%d: failed to get next order: %v", idx, err)
				return true
			}

			var orderProducts Response[[]OrderProductsResponse]
			if err := json.NewDecoder(respNextOrder.Body).Decode(&orderProducts); err != nil {
				log.Printf("worker-%d: failed to decode next order response: %v", idx, err)
				return true
			}
			respNextOrder.Body.Close()
			log.Printf("worker-%d: got next order: %v", idx, orderProducts.Data)

			if len(orderProducts.Data) == 0 {
				log.Printf("worker-%d: no more orders", idx)
				return false
			}

			// 2. Download Image of each product
			for _, product := range orderProducts.Data {
				// 2.1 Download Image
				//log.Printf("worker-%d: downloading image: %s", idx, product.CustomerImgUrl)
				//reqDownload, err := http.NewRequestWithContext(
				//	context.Background(),
				//	"GET",
				//	product.CustomerImgUrl,
				//	nil)
				//if err != nil {
				//	log.Printf("worker-%d: failed to create download request: %v", idx, err)
				//	return true
				//}
				//respDownload, err := http.DefaultClient.Do(reqDownload)
				//if err != nil {
				//	log.Printf("worker-%d: failed to download image: %v", idx, err)
				//	return true
				//}
				//
				//imageData, err := io.ReadAll(respDownload.Body)
				//if err != nil {
				//	log.Printf("worker-%d: failed to read image: %v", idx, err)
				//	return true
				//}
				//
				//respDownload.Body.Close()

				// 2.2 Process Image (final img = customer img data)
				//log.Printf("worker-%d: processing image: %s", idx, product.CustomerImgUrl)
				//finalImageData := imageData
				//contentType := http.DetectContentType(finalImageData)
				//log.Printf("worker-%d: image content type: %s", idx, contentType)

				//finalImgFilePath, ok := processedImagesMap[product.CustomerImgUrl]
				//if !ok {
				//	log.Printf("worker-%d: failed to map", idx)
				//	panic(1)
				//}
				//
				//// Load image from local file
				//finalImageData, err := os.ReadFile(finalImgFilePath)
				//if err != nil {
				//	log.Printf("worker-%d: failed to read image from %s: %v", idx, finalImgFilePath, err)
				//	return true
				//}
				//contentType := http.DetectContentType(finalImageData)
				//log.Printf("worker-%d: loaded image from %s, size: %d, content type: %s", idx, finalImgFilePath, len(finalImageData), contentType)
				//
				//////////////////////////////// REQUEST PRESIGNED URL
				//// 2.3 Upload Image to R2 (POST /image/presigned)
				//log.Printf("worker-%d: uploading image: %s", idx, product.CustomerImgUrl)
				//
				//presignedPayload, _ := json.Marshal(PresignedRequest{
				//	Key: product.FulfillmentID,
				//})
				//
				//presignedUrl := fmt.Sprintf("%s/image/presigned", url)
				//
				//reqPresigned, err := http.NewRequestWithContext(
				//	context.Background(),
				//	"POST",
				//	presignedUrl,
				//	bytes.NewBuffer(presignedPayload))
				//
				//if err != nil {
				//	log.Printf("worker-%d: failed to create presigned request: %v", idx, err)
				//	return true
				//}
				//reqPresigned.Header.Set("Authorization", "Bearer "+accessToken)
				//respPresigned, err := http.DefaultClient.Do(reqPresigned)
				//if err != nil {
				//	log.Printf("worker-%d: failed to get presigned url: %v", idx, err)
				//	return true
				//}
				//
				//var presignedResp Response[PresignedResponse]
				//if err := json.NewDecoder(respPresigned.Body).Decode(&presignedResp); err != nil {
				//	log.Printf("worker-%d: failed to decode presigned response: %v", idx, err)
				//	return true
				//}
				//
				//respPresigned.Body.Close()
				//
				//log.Printf("worker-%d: presigned url: %s, download url: %s", idx, presignedResp.Data.URL, presignedResp.Data.DownloadURL)
				//
				//reqUpload, err := http.NewRequestWithContext(
				//	context.Background(),
				//	"PUT",
				//	presignedResp.Data.URL,
				//	bytes.NewBuffer(finalImageData))
				//if err != nil {
				//	log.Printf("worker-%d: failed to create upload request: %v", idx, err)
				//	return true
				//}
				//
				//reqUpload.Header.Set("Content-Type", contentType)
				//respUpload, err := http.DefaultClient.Do(reqUpload)
				//if err != nil {
				//	log.Printf("worker-%d: failed to upload image: %v", idx, err)
				//	return true
				//}
				//
				//if respUpload.StatusCode != 200 {
				//	log.Printf("worker-%d: failed to upload image: %v", idx, respUpload.Status)
				//	return true
				//}
				//respUpload.Body.Close()
				////////////////////////////////////////////////////////////////

				//2.4 Process Order Product (POST /orders/{order_id}/products/{order_product_fulfillment_id})
				// Skip presigned
				finalImgFilePath, _ := processedImagesMapUrl[product.CustomerImgUrl]

				processOrderProductPayload, _ := json.Marshal(ProcessOrderProductRequest{
					FinalImgUrl: finalImgFilePath,
				})

				//finalImgUrl, ok := processedImagesMap[product.CustomerImgUrl]
				//if !ok {
				//	log.Printf("worker-%d: failed to map", idx)
				//	panic(1)
				//}
				//
				//processOrderProductPayload, _ := json.Marshal(ProcessOrderProductRequest{
				//	FinalImgUrl: finalImgUrl,
				//})

				log.Printf("worker-%d: processing order product: %s", idx, product.FulfillmentID)
				processOrderProductUrl := fmt.Sprintf("%s/orders/%d/products/%s",
					url, product.OrderID, product.FulfillmentID)
				reqProcess, err := http.NewRequestWithContext(
					context.Background(),
					"POST",
					processOrderProductUrl,
					bytes.NewBuffer(processOrderProductPayload))
				if err != nil {
					log.Printf("worker-%d: failed to create process request: %v", idx, err)
					return true
				}

				reqProcess.Header.Set("Authorization", "Bearer "+accessToken)
				respProcess, err := http.DefaultClient.Do(reqProcess)
				if err != nil {
					log.Printf("worker-%d: failed to process order product: %v", idx, err)
					return true
				}

				if respProcess.StatusCode != 200 {
					log.Printf("worker-%d: failed to process order product: %v", idx, respProcess.Status)
					return true
				}

				respProcess.Body.Close()
			}

			// 3. Approve Image (POST /orders/{order_id}/designer)
			log.Printf("worker-%d: approving image: %d", idx, orderProducts.Data[0].OrderID)
			approveOrderUrl := fmt.Sprintf("%s/orders/%d/designer",
				url, orderProducts.Data[0].OrderID)
			reqApprove, err := http.NewRequestWithContext(
				context.Background(),
				"POST",
				approveOrderUrl,
				nil)
			if err != nil {
				log.Printf("worker-%d: failed to create approve request: %v", idx, err)
				return true
			}

			reqApprove.Header.Set("Authorization", "Bearer "+accessToken)
			respApprove, err := http.DefaultClient.Do(reqApprove)
			if err != nil {
				log.Printf("worker-%d: failed to approve image: %v", idx, err)
				return true
			}
			if respApprove.StatusCode != 200 {
				log.Printf("worker-%d: failed to approve image: %v", idx, respApprove.Status)
				return true
			}
			respApprove.Body.Close()

			log.Printf("worker-%d: approved order designer: %d", idx, orderProducts.Data[0].OrderID)
			return true
		}()

		if processed {
			log.Printf("Worker %d processed image", idx)
			backoff = PollInterval
		} else {
			log.Printf("Worker %d no more image to process", idx)
			backoff = min(backoff*2, 5*time.Minute) // cap at 5 minutes
		}

		numProcessedOrder++
		time.Sleep(backoff)

	}

	wg.Done()

}
