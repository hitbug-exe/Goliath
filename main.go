package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
  "math/rand"
  "hash/fnv"
  "math"
)

// The const block defines two constant variables, Attempts and Retry. Attempts has a value of 0, and Retry has a value of 1. These variables will be used to index into the backend slice when choosing a backend for a request.
const (
	Attempts int = iota
	Retry
)

// The Backend struct holds data about a server, including its URL, weight, number of connections, whether it's currently alive, a mutex for concurrency, and a reverse proxy for forwarding requests.
type Backend struct {
	URL          *url.URL
  Weight       int64
  Connections  int64
	Alive        bool
	mux          sync.RWMutex
	ReverseProxy *httputil.ReverseProxy
}

// SetAlive for this backend
// SetAlive sets the "alive" status of the backend to the provided boolean value
// Args:
// - alive (bool): a boolean value representing whether the backend is alive or not
func (b *Backend) SetAlive(alive bool) {
b.mux.Lock() // acquire the lock to ensure atomicity
b.Alive = alive // set the "alive" status of the backend
b.mux.Unlock() // release the lock
}

// SetWeight sets the weight of the backend to the provided value
// Args:
// - weight (int64): an integer value representing the weight of the backend
// Returns: none
func (b *Backend) SetWeight(weight int64) {
b.mux.Lock() // acquire the lock to ensure atomicity
b.Weight = weight // set the weight of the backend
b.mux.Unlock() // release the lock
return
}

// IsAlive returns a boolean value indicating whether the backend is alive or not
// Returns:
// - alive (bool): a boolean value indicating whether the backend is alive or not
func (b *Backend) IsAlive() (alive bool) {
b.mux.RLock() // acquire the read lock to ensure concurrent access
alive = b.Alive // get the "alive" status of the backend
b.mux.RUnlock() // release the read lock
return
}

// ServerPool holds information about reachable backends and the load balancing policy
type ServerPool struct {
backends []*Backend // a slice of backends that are reachable
current uint64 // the index of the currently selected backend
policy uint64 // the load balancing policy to use
// 0 -> Round Robin, 1 -> Weighted Round Robin, 2 -> Random, 3 -> URL hash, 4-> LeastConnections
}

// Example uses:
// - To create a new server pool with the default round-robin policy:
// sp := ServerPool{backends: []*Backend{backend1, backend2, backend3}, current: 0, policy: 0}
// - To get the next available backend according to the load balancing policy:
// backend := sp.NextBackend()
// - To set the load balancing policy to "least connections":
// sp.SetPolicy(4)



// AddBackend adds a new Backend to the ServerPool's slice of backends.
// Args:
// - backend: a pointer to the Backend instance to be added
// Returns: nothing
// Example use:
// backend := NewBackend("http://localhost:8080")
// serverPool.AddBackend(backend)
func (s *ServerPool) AddBackend(backend *Backend) {
  s.backends = append(s.backends, backend)
}

// NextIndex atomically increases the counter and returns an index to the next available backend.
// Returns: an integer index representing the next backend to use
// Example use:
// index := serverPool.NextIndex()
// backend := serverPool.backends[index]
func (s *ServerPool) NextIndex() int {
  return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.backends)))
}

// MarkBackendStatus changes the status of a backend in the ServerPool.
// Args:
// - backendUrl: a pointer to a url.URL instance representing the backend to update
// - alive: a boolean indicating whether the backend is alive or not
// Returns: nothing
// Example use:
// url, _ := url.Parse("http://localhost:8080")
// serverPool.MarkBackendStatus(url, true)
func (s *ServerPool) MarkBackendStatus(backendUrl *url.URL, alive bool) {
  for _, b := range s.backends {
    if b.URL.String() == backendUrl.String() {
      b.SetAlive(alive)
      break
    }  
  }
}

// RRGetNextPeer returns the next active peer to take a connection using round-robin algorithm
// Args:
// s *ServerPool: a pointer to the ServerPool struct
// Returns:
// *Backend: a pointer to the next active backend to take a connection
//
// Example usage:
// pool := &ServerPool{backends: []*Backend{backend1, backend2}}
// nextPeer := pool.RRGetNextPeer()

func (s *ServerPool) RRGetNextPeer() *Backend {
// Loop entire backends to find out an alive backend
  next := s.NextIndex()
  l := len(s.backends) + next // Start from next and move a full cycle
  for i := next; i < l; i++ {
    idx := i % len(s.backends) // Take an index by modding
    if s.backends[idx].IsAlive() { // If we have an alive backend, use it and store if it's not the original one
      if i != next {
      atomic.StoreUint64(&s.current, uint64(idx)) // Store the current index using atomic operation
    }
    return s.backends[idx]
    }
  }
  return nil // If no alive backend is found, return nil
}

// WRRGetNextPeer returns the next available backend server using a weighted round-robin algorithm
// Args: s *ServerPool - a pointer to a ServerPool struct containing a slice of Backend structs
// Returns: *Backend - a pointer to the selected backend server
func (s *ServerPool) WRRGetNextPeer() *Backend {
	// Initialize variables
	totalWeight := int64(0)
	var maxWeight int64 = 0
	var selectedBackend *Backend

	// Find the total weight of all backends
	for _, backend := range s.backends {
		totalWeight += backend.Weight
		if backend.Weight > maxWeight {
			maxWeight = backend.Weight
		}
	}

	// If all backends have zero weight, return nil
	if totalWeight == 0 {
		return nil
	}

	// Choose the next backend using a weighted round-robin algorithm
	for {
		// Get the next index from the pool
		next := s.NextIndex()

		// Loop through all backends to select the next available server
		for i := 0; i < len(s.backends); i++ {
			// Calculate the index of the backend server
			idx := (next + i) % len(s.backends)
			backend := s.backends[idx]
			// Check if the backend server is alive
			if backend.IsAlive() {
				// If the backend's weight is greater than or equal to the maximum weight, select it
				if backend.Weight >= maxWeight {
					selectedBackend = backend
					break
				}
				// Otherwise, use a random number to select a backend based on its weight
				if rand.Int63n(totalWeight) < backend.Weight {
					selectedBackend = backend
					break
				}
			}
		}
		// If a backend server has been selected, update the current index and return the backend server
		if selectedBackend != nil {
			atomic.StoreUint64(&s.current, uint64((s.current+1)%uint64(len(s.backends))))
			return selectedBackend
		}
	}
}


// RandomGetNextPeer returns a random alive backend from the server pool
// Arguments:
//   - s: pointer to the ServerPool struct
// Returns:
//   - a pointer to the selected Backend struct or nil if no alive backends exist
func (s *ServerPool) RandomGetNextPeer() *Backend {
	// Seed the random number generator with the current time
	rand.Seed(time.Now().UnixNano())

	// Create an empty slice to hold all alive backends
	aliveBackends := make([]*Backend, 0)

	// Iterate through all the backends in the server pool
	for _, b := range s.backends {
		// If the backend is alive, append it to the aliveBackends slice
		if b.IsAlive() {
			aliveBackends = append(aliveBackends, b)
		}
	}

	// If there are no alive backends, return nil
	if len(aliveBackends) == 0 {
		return nil
	}

	// Select a random backend from the list of alive backends
	randomIndex := rand.Intn(len(aliveBackends))
	return aliveBackends[randomIndex]
}

// HashGetNextPeer returns a backend from the server pool based on a hash of the request URL
// Arguments:
//   - s: pointer to the ServerPool struct
//   - r: pointer to the http.Request struct
// Returns:
//   - a pointer to the selected Backend struct or nil if no alive backends exist
func (s *ServerPool) HashGetNextPeer(r *http.Request) *Backend {
	// Create a new FNV-1a hash object
	hash := fnv.New32a()

	// Hash the request URL
	_, _ = hash.Write([]byte(r.URL.String()))

	// Convert the hash to an index into the backend pool
	index := int(hash.Sum32()) % len(s.backends)

	// Find the first alive backend starting from the index
	for i := 0; i < len(s.backends); i++ {
		// Calculate the index of the current backend
		idx := (index + i) % len(s.backends)
		// If the backend is alive, return it
		if s.backends[idx].IsAlive() {
			return s.backends[idx]
		}
	}

	// If no alive backend is found, return nil
	return nil
}


// Function Name: LeastConnectionsGetNextPeer
// Args: None
// Returns: *Backend - the next available backend with the least active connections.

// LeastConnectionsGetNextPeer iterates through the ServerPool's list of backends and returns the next available backend with the least number of active connections. If all the backends are dead, it returns nil.

// Example Use:
// 1. backend := sp.LeastConnectionsGetNextPeer()
// This will return the next available backend with the least number of active connections, from the ServerPool sp.

func (s *ServerPool) LeastConnectionsGetNextPeer() *Backend {
var leastConnBackend *Backend // A pointer to the backend with the least number of active connections.
minConnections := int64(math.MaxInt64) // Set minConnections to the maximum possible value of int64.
// Iterate through each backend in the ServerPool.
for _, b := range s.backends {
	// If the backend is not alive, skip it.
	if !b.IsAlive() {
		continue
	}

	// If the backend has fewer active connections than minConnections, update the leastConnBackend to this backend and update minConnections.
	if b.Connections < minConnections {
		leastConnBackend = b
		minConnections = b.Connections
	}
}

// If no backend with an active connection was found, return nil.
if leastConnBackend == nil {
	return nil
}

// Increase the number of active connections for the leastConnBackend.
leastConnBackend.mux.RLock()
leastConnBackend.Connections = leastConnBackend.Connections + 1
leastConnBackend.mux.RUnlock()

// Return the leastConnBackend.
return leastConnBackend
}

// ServerPool represents a pool of servers to load balance between.
type ServerPool struct {
	backends []*Backend // slice of backends to load balance between
	mux      sync.RWMutex // lock to protect the backends slice
}

// Backend represents a backend server to load balance to.
type Backend struct {
	URL         *url.URL // the URL of the backend server
	Alive       bool // whether the backend server is currently alive
	mux         sync.RWMutex // lock to protect the Alive field
	ReverseProxy *httputil.ReverseProxy // the reverse proxy for this backend server
}

// HealthCheck pings the backends and updates their status based on the result.
func (s *ServerPool) HealthCheck() {
	for _, b := range s.backends {
		status := "up" // assume the backend server is up by default
		alive := isBackendAlive(b.URL) // check if the backend server is alive
		b.SetAlive(alive) // update the backend's alive status
		if !alive {
			status = "down" // update the status to "down" if the backend server is not alive
		}
		log.Printf("%s [%s]\n", b.URL, status) // log the backend server's status
	}
}

// GetAttemptsFromContext returns the number of attempts made for a request.
// It looks for the Attempts key in the request context and returns its value if present,
// otherwise it returns 1, indicating that this is the first attempt.
func GetAttemptsFromContext(r *http.Request) int {
	if attempts, ok := r.Context().Value(Attempts).(int); ok {
		return attempts
	}
	return 1 // if the Attempts key is not present in the context, assume this is the first attempt
}

// GetRetryFromContext returns the number of retries attempted for a request.
// It looks for the Retry key in the request context and returns its value if present,
// otherwise it returns 0, indicating that no retries have been attempted yet.
func GetRetryFromContext(r *http.Request) int {
	if retry, ok := r.Context().Value(Retry).(int); ok {
		return retry
	}
	return 0 // if the Retry key is not present in the context, assume no retries have been attempted yet
}



/ lb is a function that load balances incoming HTTP requests to backend servers.
// It takes in two arguments:
// - w: an http.ResponseWriter, used to write the response to the client.
// - r: an *http.Request, representing the incoming HTTP request.
// It returns nothing.

func lb(w http.ResponseWriter, r *http.Request) {
// Get the number of attempts made to access this resource from the request context.
attempts := GetAttemptsFromContext(r)
// If the number of attempts is greater than 3, log the attempt and return a service unavailable error.
if attempts > 3 {
	log.Printf("%s(%s) Max attempts reached, terminating\n", r.RemoteAddr, r.URL.Path)
	http.Error(w, "Service not available", http.StatusServiceUnavailable)
	return
}

var peer *Backend = nil
policy := serverPool.policy

// Select a backend server based on the load balancing policy.
switch policy {
case 0:
	peer = serverPool.RRGetNextPeer() // Round Robin policy
case 1:
	peer = serverPool.WRRGetNextPeer() // Weighted Round Robin policy
case 2:
	peer = serverPool.RandomGetNextPeer() // Random policy
case 3:
	peer = serverPool.HashGetNextPeer(r) // Hash-based policy
case 4:
	peer = serverPool.LeastConnectionsGetNextPeer() // Least Connections policy
default:
	peer = serverPool.RRGetNextPeer()
}

// If a backend server was found, serve the request through the reverse proxy.
if peer != nil {
	peer.ReverseProxy.ServeHTTP(w, r)
	return
}

// Otherwise, return a service unavailable error.
http.Error(w, "Service not available", http.StatusServiceUnavailable)
  
}

// isAlive checks whether a backend is alive by establishing a TCP connection
// Args:
//   u (*url.URL): a pointer to a url.URL object representing the backend to check
// Returns:
//   bool: true if the backend is alive, false otherwise
func isBackendAlive(u *url.URL) bool {
	// Set a timeout of 2 seconds for establishing the TCP connection
	timeout := 2 * time.Second
	// Establish a TCP connection to the backend
	conn, err := net.DialTimeout("tcp", u.Host, timeout)
	// If there is an error establishing the connection, log the error and return false
	if err != nil {
		log.Println("Site unreachable, error: ", err)
		return false
	}
	// Close the connection before returning true
	defer conn.Close()
	return true
}

// healthCheck runs a routine for checking the status of the backends every 2 minutes
func healthCheck() {
	// Create a new ticker that ticks every 2 minutes
	t := time.NewTicker(time.Minute * 2)
	for {
		select {
		case <-t.C:
			// Log that a health check is starting
			log.Println("Starting health check...")
			// Run the HealthCheck function on the server pool to check the status of the backends
			serverPool.HealthCheck()
			// Log that the health check is complete
			log.Println("Health check completed")
		}
	}
}

// Example use of isBackendAlive:
// urlToCheck, _ := url.Parse("http://example.com")
// isAlive := isBackendAlive(urlToCheck)
// fmt.Println("Backend is alive:", isAlive)

// Example use of healthCheck:
// go healthCheck() // start the health check routine in a separate goroutine


var serverPool ServerPool

func main() {
// Declare and initialize variables used for command line arguments.
var serverList string // Comma-separated list of backend servers
var port int // Port on which to serve
var policy string // Load balancing policy
// Define command line arguments.
flag.StringVar(&serverList, "backends", "", "Load balanced backends, use commas to separate")
flag.IntVar(&port, "port", 3030, "Port to serve")
flag.StringVar(&policy, "policy", "rr", "Load balancing policy (rr, wrr, rand, uhash, lc)")

// Parse command line arguments.
flag.Parse()

// If no backends are provided, exit with an error.
if len(serverList) == 0 {
	log.Fatal("Please provide one or more backends to load balance")
}

// Parse servers from the comma-separated list.
tokens := strings.Split(serverList, ",")
for _, tok := range tokens {
	serverUrl, err := url.Parse(tok)
	if err != nil {
		log.Fatal(err)
	}

	// Create a new reverse proxy for the backend server.
	proxy := httputil.NewSingleHostReverseProxy(serverUrl)

	// Set an error handler for the proxy.
	proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, e error) {
		log.Printf("[%s] %s\n", serverUrl.Host, e.Error())

		// Get the number of retries for the request.
		retries := GetRetryFromContext(request)

		// If the number of retries is less than 3, retry after a short delay.
		if retries < 3 {
			select {
			case <-time.After(10 * time.Millisecond):
				ctx := context.WithValue(request.Context(), Retry, retries+1)
				proxy.ServeHTTP(writer, request.WithContext(ctx))
			}
			return
		}

		// After 3 retries, mark the backend as down.
		serverPool.MarkBackendStatus(serverUrl, false)

		// If the same request has been routed to different backends multiple times, increase the count.
		attempts := GetAttemptsFromContext(request)
		log.Printf("%s(%s) Attempting retry %d\n", request.RemoteAddr, request.URL.Path, attempts)
		ctx := context.WithValue(request.Context(), Attempts, attempts+1)
		lb(writer, request.WithContext(ctx))
	}

	// Create a new backend with weight and connections initialized.
	serverPool.AddBackend(&Backend{
		URL:          serverUrl,
		Weight:       1, // Default weight to 1
		Connections:  0,
		Alive:        true,
		ReverseProxy: proxy,
	})
	log.Printf("Configured server: %s\n", serverUrl)
}

// Set the load balancing policy based on the command line argument.
switch policy {
case "rr":
	serverPool.policy = 0
case "wrr":
	serverPool.policy = 1
case "rand":
	serverPool.policy = 2
case "uhash":
	serverPool.policy = 3
case "lc":
	serverPool.policy = 4
default:
	log.Fatalf("Unknown policy %s\n", policy)
}

// Create an HTTP server.
server := http.Server{
	Addr:    fmt.Sprintf(":%d", port),
	Handler: http.HandlerFunc(lb),
}

// Start health checking in a separate goroutine.
go healthCheck()

// Start the HTTP server.
log.Printf("Load Balancer started at :%d\n", port)
if err := server.ListenAndServe(); err != nil {
	log.Fatal(err)
	}
}




