package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Message struct {
	ID      int    `json:"id"`
	Message string `json:"message"`
	Task    Task   `json:"task"`
}

type Task struct {
	ID            int    `json:"id"`
	Task          string `json:"task"`
	URL           string `json:"url"`
	SleepDuration int    `json:"sleepDuration"`
}

var (
	messages     = make(map[int]Message)
	nextID       = 1
	messagesMu   sync.Mutex
	taskQueue    = make(chan Task, 1000)
	counterChan  = make(chan int)
	wg           sync.WaitGroup
	taskCounter  int
	counterMutex sync.Mutex
)

func main() {
	http.HandleFunc("/run/", runHandler)
	http.HandleFunc("/wait/", waitHandler)
	http.HandleFunc("/messages/", messageHandler)
	http.HandleFunc("/count/", countHandler)

	go counter()

	for i := 0; i < 10000; i++ {
		go worker()
	}

	fmt.Println("Server is running on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func waitHandler(w http.ResponseWriter, r *http.Request) {
	wg.Wait() // Wait for all tasks to complete

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "all tasks completed"})
}

func worker() {
	for task := range taskQueue {
		fmt.Println("Task received: ", task.ID)
		wg.Add(1)

		go func(t Task) {
			defer wg.Done()
			processTask(t)

			counterChan <- 1
		}(task)
	}
}

func processTask(t Task) {
	if t.URL != "" {
		resp, err := http.Get(t.URL)
		if err != nil {
			fmt.Printf("Error fetching URL %s: %v\n", t.URL, err)
		} else {
			fmt.Printf("Fetched URL %s: %s\n", t.URL, resp.Status)
			resp.Body.Close()
		}
	}

	if t.SleepDuration > 0 {
		fmt.Printf("Sleeping for %d seconds\n", t.SleepDuration)
		time.Sleep(time.Duration(t.SleepDuration) * time.Second)
	}

	fmt.Printf("Task completed: %d\n", t.ID)
}

func counter() {
	for increment := range counterChan {
		taskCounter += increment
	}
}

func incrementCounter() {
	counterMutex.Lock()
	defer counterMutex.Unlock()

	taskCounter++

	fmt.Printf("Task counter incremented: %d\n", taskCounter)
}

func countHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"taskCounter": taskCounter})
}

func runHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		handleRun(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleRun(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Task  Task `json:"task"`
		Count int  `json:"count"`
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	if err := json.Unmarshal(body, &request); err != nil {
		http.Error(w, "Error parsing request body", http.StatusBadRequest)
		return
	}

	messagesMu.Lock()
	defer messagesMu.Unlock()

	for i := 0; i < request.Count; i++ {
		taskQueue <- request.Task
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "tasks queued"})
}

func messageHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		handleGetMessage(w, r)
	case "POST":
		handlePostMessage(w, r)
	case "DELETE":
		handleDeleteMessage(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleGetMessage(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.URL.Path[len("/messages/"):])
	if err != nil {
		http.Error(w, "Invalid message ID", http.StatusBadRequest)
		return
	}

	messagesMu.Lock()
	defer messagesMu.Unlock()

	p, ok := messages[id]
	if !ok {
		http.Error(w, "Message not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(p)
}

func handlePostMessage(w http.ResponseWriter, r *http.Request) {
	var m Message

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	if err := json.Unmarshal(body, &m); err != nil {
		http.Error(w, "Error parsing request body", http.StatusBadRequest)
		return
	}

	messagesMu.Lock()
	defer messagesMu.Unlock()

	m.ID = nextID
	nextID++
	messages[m.ID] = m

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(m)
}

func handleDeleteMessage(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.URL.Path[len("/messages/"):])
	if err != nil {
		http.Error(w, "Invalid message ID", http.StatusBadRequest)
		return
	}

	messagesMu.Lock()
	defer messagesMu.Unlock()

	// If you use a two-value assignment for accessing a
	// value on a map, you get the value first then an
	// "exists" variable.
	_, ok := messages[id]
	if !ok {
		http.Error(w, "Message not found", http.StatusNotFound)
		return
	}

	delete(messages, id)
	w.WriteHeader(http.StatusOK)
}
