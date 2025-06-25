package main

import (
	"fmt"
	"sync"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func SerialCrawl(url string, depth int, fetcher Fetcher) {
	if depth <= 0 {
		return
	}
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("found: %s %q\n", url, body)
	for _, u := range urls {
		SerialCrawl(u, depth-1, fetcher)
	}
	return
}

// How can we parallelize the crawler?
// Goroutine and channels
// sync pkg with condition variables and mutex
//
// CHANNELS
// Master/worker thread pool idea, with a main function that calls both
//
// Process urls
func channelWorker(url string, fetcher Fetcher, urlStream chan<- []string) {
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		// better error handling in future
		urlStream <- []string{"err"}
	} else {
		fmt.Printf("found: %s %q\n", url, body)
		urlStream <- urls
	}
	// No more writes to the urlStream
}

// Coordinate crawl strategy, tracking how deep each URL is in the crawl tree.
func master(urlStream chan []string, fetcher Fetcher) {
	depth := 1
	// Only accessed w/in coordinator, thus safe
	seen := make(map[string]struct{})

	// Keep reading the channel until depth reaches 0
	// This is our stop condition (as opposed to a done channel or the like)
	for urls := range urlStream {
		for _, url := range urls {
			if _, ok := seen[url]; !ok {
				seen[url] = struct{}{}
				depth++
				go channelWorker(url, fetcher, urlStream)
			}
		}
		depth--
		if depth <= 0 {
			break
		}
	}
}

func ChannelCrawl(url string, fetcher Fetcher) {
	// Create a bi-directional channel
	ch := make(chan []string)
	// We need to launch a goroutine here since this is a blocking channel;
	// if we simply write to the channel, we'll ALSO wait for a receiver.
	// This will then block until the master goroutine runs the worker channel,
	// which will then write to this goroutine.
	// NOTE: Create a coordinator/master thread by first *CREATING* the channel
	// and passing this channel into the coordinator with the BEGINNING url.
	go func() {
		// Send the single url
		// Initialize a string slice with single element url
		ch <- []string{url}
	}()
	master(ch, fetcher)
}

/*
func channelWorker(url string, fetcher Fetcher, resultStream chan<- []string) {
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		// better error handling in future
		resultStream <- []string{"err"}
	} else {
		fmt.Printf("found: %s %q\n", url, body)
		resultStream <- urls
	}
}

// Instead, ChannelCrawl will also pose as Master, indicating channel ownership
func ChannelCrawl(startUrl string, fetcher Fetcher) {
	urlStream := make(chan []string)

	go func() {
		urlStream <- []string{startUrl}
	}()

	seen := make(map[string]struct{})
	depth := 1
	for urls := range urlStream {
		for _, url := range urls {
			if _, ok := seen[url]; !ok {
				seen[url] = struct{}{}
				depth++
				go channelWorker(url, fetcher, urlStream)
			}
			depth--
			if depth <= 0 {
				break
			}
		}
	}
}
*/

type fetchState struct {
	mu      sync.Mutex
	fetched map[string]bool
}

func ConcurrentMutex(url string, fetcher Fetcher, f *fetchState) {
	f.mu.Lock()
	already := f.fetched[url]
	f.fetched[url] = true
	f.mu.Unlock()

	if already {
		return
	}

	_, urls, err := fetcher.Fetch(url)
	if err != nil {
		return
	}
	var done sync.WaitGroup
	for _, u := range urls {
		done.Add(1)
		//u2 := u
		//go func() {
		// defer done.Done()
		// ConcurrentMutex(u2, fetcher, f)
		//}()
		go func(u string) {
			defer done.Done()
			ConcurrentMutex(u, fetcher, f)
		}(u)
	}
	done.Wait()
	return
}

func makeState() *fetchState {
	f := &fetchState{}
	f.fetched = make(map[string]bool)
	return f
}

func main() {
	/*
		fmt.Println("SERIAL")
		SerialCrawl("https://golang.org/", 4, fetcher)
	*/
	fmt.Println("CHANNEL")
	ChannelCrawl("https://golang.org/", fetcher)
	// ConcurrentMutex("https://golang.org/", )
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
