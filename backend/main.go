package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Payment struct {
	CorrelationID string    `json:"correlationId"`
	Amount        float64   `json:"amount"`
	Timestamp     time.Time `json:"timestamp"`
	Service       string    `json:"service"`
}

type Summary struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type Server struct {
	mu       sync.RWMutex
	payments []Payment

	DefaultURL  string
	FallbackURL string
}

func (s *Server) processPayment(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CorrelationID string  `json:"correlationId"`
		Amount        float64 `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if _, err := uuid.Parse(req.CorrelationID); err != nil {
		http.Error(w, "invalid correlationId", http.StatusBadRequest)
		return
	}

	serviceURL := s.DefaultURL
	dest := "default"
	resp, err := s.forwardPayment(serviceURL, req)
	if err != nil || resp.StatusCode >= 500 {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		serviceURL = s.FallbackURL
		dest = "fallback"
		resp, err = s.forwardPayment(serviceURL, req)
		if err != nil {
			http.Error(w, "processor error", http.StatusBadGateway)
			return
		}
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	s.mu.Lock()
	s.payments = append(s.payments, Payment{
		CorrelationID: req.CorrelationID,
		Amount:        req.Amount,
		Timestamp:     time.Now().UTC(),
		Service:       dest,
	})
	s.mu.Unlock()

	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

func (s *Server) forwardPayment(url string, req interface{}) (*http.Response, error) {
	b, _ := json.Marshal(req)
	httpReq, _ := http.NewRequest("POST", fmt.Sprintf("%s/payments", url), bytes.NewReader(b))
	httpReq.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 5 * time.Second}
	return client.Do(httpReq)
}

func (s *Server) paymentsSummary(w http.ResponseWriter, r *http.Request) {
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")
	var from, to time.Time
	var err error
	if fromStr != "" {
		from, err = time.Parse(time.RFC3339, fromStr)
		if err != nil {
			http.Error(w, "invalid from", http.StatusBadRequest)
			return
		}
	}
	if toStr != "" {
		to, err = time.Parse(time.RFC3339, toStr)
		if err != nil {
			http.Error(w, "invalid to", http.StatusBadRequest)
			return
		}
	}
	if to.IsZero() {
		to = time.Now().UTC()
	}

	sum := map[string]Summary{"default": {}, "fallback": {}}

	s.mu.RLock()
	for _, p := range s.payments {
		if !from.IsZero() && p.Timestamp.Before(from) {
			continue
		}
		if !to.IsZero() && p.Timestamp.After(to) {
			continue
		}
		entry := sum[p.Service]
		entry.TotalRequests++
		entry.TotalAmount += p.Amount
		sum[p.Service] = entry
	}
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]Summary{
		"default":  sum["default"],
		"fallback": sum["fallback"],
	})
}

func main() {
	defaultURL := env("DEFAULT_URL", "http://payment-processor-default:8080")
	fallbackURL := env("FALLBACK_URL", "http://payment-processor-fallback:8080")

	srv := &Server{DefaultURL: defaultURL, FallbackURL: fallbackURL}

	http.HandleFunc("/payments", srv.processPayment)
	http.HandleFunc("/payments-summary", srv.paymentsSummary)

	addr := env("ADDR", ":9999")
	log.Printf("listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
