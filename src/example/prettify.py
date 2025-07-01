#!/usr/bin/env python3
"""
Simple log prettifier for Go structured logs.
Usage:
    VERBOSE=3 go run logging.go | python3 prettify.py
    VERBOSE=5 go run logging.go | python3 prettify.py --ignore TRCE
"""

import sys
import argparse

try:
    from rich import print as rprint
except ImportError:
    print("Install rich: pip install rich")
    sys.exit(1)

# Colors for each log level
COLORS = {
    "ERRO": "red",
    "WARN": "yellow",
    "INFO": "green",
    "DEBG": "blue",
    "TRCE": "bright_black",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ignore", help="Ignore topics (comma-separated)")
    args = parser.parse_args()

    ignore_topics = set()
    if args.ignore:
        ignore_topics = set(args.ignore.split(","))

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            # Parse: TIME TOPIC MESSAGE (3 parts, not 4)
            parts = line.split(" ", 2)  # Split into max 3 parts
            if len(parts) < 3:
                rprint(line)  # Print non-structured lines as-is
                continue

            time_str, topic, message = parts

            if topic in ignore_topics:
                continue

            # Colorize based on topic
            if topic in COLORS:
                color = COLORS[topic]
                colored_msg = f"[{color}]{message}[/{color}]"
            else:
                colored_msg = message

            rprint(
                f"[dim]{time_str}[/dim] [{COLORS.get(topic, 'white')}]{topic}[/{COLORS.get(topic, 'white')}] {colored_msg}"
            )

        except Exception as e:
            rprint(line)  # Print unparseable lines as-is


if __name__ == "__main__":
    main()
