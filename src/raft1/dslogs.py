#!/usr/bin/env python3
"""
Simple log prettifier for Go structured logs.
Usage:
    # You can just pipe the go test output into the script
    $ VERBOSE=1 go test -run InitialElection | dslogs
    # ... colored output will be printed

    # We can ignore verbose topics like timers or log changes
    $ VERBOSE=1 go test -run Backup | dslogs -c 5 -i TIMR,DROP,LOG2
    # ... colored output in 5 columns

    # Dumping output to a file can be handy to iteratively
    # filter topics and when failures are hard to reproduce
    $ VERBOSE=1 go test -run Figure8Unreliable > output.log
    # Print from a file, selecting just two topics
    $ dslogs output.log -j CMIT,PERS
    # ... colored output
"""

import sys
import argparse
from rich.console import Console
from rich.columns import Columns


# logError  logTopic = "ERRO"
# logWarn   logTopic = "WARN"
# logCommit logTopic = "CMIT"
# logInfo   logTopic = "INFO"
# logVote   logTopic = "VOTE"
# logLeader logTopic = "LEAD"
# logTimer  logTopic = "TIMR"
# logTerm   logTopic = "TERM"

TOPICS = {
    "ERRO": "red",
    "WARN": "bright_yellow",
    "TRCE": "red",
    "CMIT": "magenta",
    "INFO": "bright_white",
    "VOTE": "bright_cyan",
    "TIMR": "navy_blue",
    "TERM": "green",
}


def main():
    # [...] # Some boring command line parsing
    parser = argparse.ArgumentParser(description="Pretty print Raft logs")
    parser.add_argument(
        "file",
        nargs="?",
        type=argparse.FileType("r"),
        help="Log file to read (default: stdin)",
    )
    parser.add_argument(
        "-c",
        "--columns",
        type=int,
        dest="n_columns",
        help="Number of columns for multi-server output",
    )
    parser.add_argument("-j", "--just", nargs="+", help="Show only these topics")
    parser.add_argument("-i", "--ignore", nargs="+", help="Ignore these topics")
    parser.add_argument(
        "--no-color",
        action="store_false",
        dest="colorize",
        help="Disable colored output",
    )

    args = parser.parse_args()

    file = args.file
    n_columns = args.n_columns
    just = args.just
    ignore = args.ignore
    colorize = args.colorize

    topics = list(TOPICS.keys())

    # We can take input from a stdin (pipes) or from a file
    input_ = file if file else sys.stdin

    # Print just some topics or exclude some topics
    if just:
        topics = just
    if ignore:
        topics = [lvl for lvl in topics if lvl not in set(ignore)]

    topics = set(topics)
    console = Console()
    width = console.size.width

    panic = False
    for line in input_:
        i = -1
        try:
            time = int(line[6:])
            topic = line[7:11]
            msg = line[12:].strip()
            if topic not in topics:
                continue

            # Debug() calls from the test suite aren't associated with any particular
            # peer. Otherwise we can treat second column as peer id.
            if topic != "TEST" and n_columns:
                i = int(msg[1])
                msg = msg[3:]

            # Colorize output
            if colorize and topic in TOPICS:
                color = TOPICS[topic]
                msg = f"[{color}{msg}[/{color}]"

            # Single column. Always the case for debug calls in tests.
            if n_columns is None or topic == "TEST":
                print(time, msg)
            # Multi column printing. Timing is dropped to maximize horizontal space.
            else:
                cols = ["" for _ in range(n_columns)]
                msg = "" + msg
                cols[i] = msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1, equal=True, expand=True)
                print(cols)
        except:
            if line.startswith("panic"):
                panic = True

            if not panic:
                print("-" * console.width)
            print(line, end="")


if __name__ == "__main__":
    main()
