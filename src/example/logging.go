package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// NOTE: If you want to just run this, log by default prints to stderr,
// so you need to write the stderr fd to stdout, i.e., 2>&1.

// The MIT test case suite will use go test, so we can't grab cli args directly.
// What we CAN do instead, is use environment variables, where this function
// wil access the VERBOSE environment variable to decide whether to use
// verbose logs.
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

// Type LogTopic represents a topic that encodes a category a logged message
// belongs to. Topics relate to different parts of the implementation,
// offering a fine-grained solution for simpler grepping.
type LogTopic string

const (
	dError LogTopic = "ERRO"
	dWarn  LogTopic = "WARN"
	dInfo  LogTopic = "INFO"
	dDebug LogTopic = "DEBG"
	dTrace LogTopic = "TRCE"
)

// Convenience functions for each level
func LogError(format string, args ...any) { Debug(dError, 1, format, args...) }
func LogWarn(format string, args ...any)  { Debug(dWarn, 2, format, args...) }
func LogInfo(format string, args ...any)  { Debug(dInfo, 3, format, args...) }
func LogDebug(format string, args ...any) { Debug(dDebug, 4, format, args...) }
func LogTrace(format string, args ...any) { Debug(dTrace, 5, format, args...) }

var (
	debugStart time.Time
	debugLevel int
)

func init() {
	debugLevel = getVerbosity()
	debugStart = time.Now()
	log.SetFlags(0) // remove default timestamp
}

// Debug is a function that is used to dump logging output.
// It prints a message along with a topic and amount of milliseconds
// since the start of the run.
func Debug(topic LogTopic, level int, format string, a ...any) {
	if debugLevel >= level {
		elapsed := time.Since(debugStart).Milliseconds()
		prefix := fmt.Sprintf("%06d %s ", elapsed, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

// Emulate a basic distributed service as a logical example
func processTask(id int, data string) {
	LogInfo("Starting task %d", id)

	if data == "" {
		LogError("Task %d failed: empty data", id)
		return
	}

	if len(data) > 100 {
		LogWarn("Task %d has large data (%d bytes)", id, len(data))
	}

	LogDebug("Task %d processing data: %s", id, data)

	// Simulate work
	time.Sleep(time.Millisecond * 50)
	LogTrace("Task %d intermediate step complete", id)

	time.Sleep(time.Millisecond * 30)
	LogInfo("Task %d completed successfully", id)
}

func main() {
	LogInfo("Service starting up")

	processTask(1, "hello world")
	processTask(2, "")
	processTask(3, "hi mom")
	processTask(4, "askdjflkasjdfkljlkjkljalksjfoiejoijdslkfasdlkfjkasdjfkajsdlkfjaslkdjflksjdflkjaslkdjflkasjdlfkjaslkdjfklasjdlkfjaslkdjflkasjdklfjlsakjflksdjlkfjlskadfklflakdsjflkeoiqjpnfdncvzncxzjd")

	LogInfo("Service shutting down")
}
