package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
/*
const Debug = false

func DPrintf(format string, a ...any) {
	if Debug {
		log.Printf(format, a...)
	}
}
*/

// Taken from this article here: https://blog.josejg.com/debugging-pretty/

// Retrieves a given verbosity level from an environment variable for use with
// a prettify python script to colorify our Go log output.
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

// Type logTopic represents a string type for logging Raft output.
type logTopic string

// A collection of logTopics that will be parsed by our pretty printer.
const (
	// Error
	logError logTopic = "ERRO"
	// Warn
	logWarn logTopic = "WARN"
	// Trace
	logTrace logTopic = "TRCE"
	// Commit
	logCommit logTopic = "CMIT"
	// Info
	logInfo logTopic = "INFO"
	// Debug
	logDebug logTopic = "DBUG"
	// Vote
	logVote logTopic = "VOTE"
	// Leader
	logLeader logTopic = "LEAD"
	// Long running timer
	logTimer logTopic = "TIMR"
	// Term
	logTerm logTopic = "TERM"
)

// global variables for use by Debug
var (
	debugStart     time.Time
	debugVerbosity int
)

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() & ^(log.Ldate | log.Ltime))
}

// Debug dumps logging output dependent on a verbosity environment variable sent in.
// It prints a message along with a Raft topic and the amount of milliseconds since the start
// of the run.
func Debug(topic logTopic, format string, msg ...any) {
	if debugVerbosity >= 1 {
		elapsed := time.Since(debugStart).Milliseconds()
		prefix := fmt.Sprintf("%06d %s ", elapsed, string(topic))
		format = prefix + format
		log.Printf(format, msg...)
	}
	// NOTE: We pass in exactly WHO is sending the message as part of the message when calling Debug.
	//
	// I.e., Debug(logTimer, "S%d Leader, checking heartbeats", rf.me)
}
