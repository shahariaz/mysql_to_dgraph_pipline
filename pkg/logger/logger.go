// Package logger provides structured logging functionality for the MySQL to Dgraph pipeline.
// It wraps logrus to provide consistent logging with configurable levels and formats.
package logger

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

// Logger wraps logrus.Logger with additional convenience methods for structured logging
type Logger struct {
	*logrus.Logger
}

// New creates a new logger instance with specified level and format
func New(level, format string) *Logger {
	log := logrus.New()

	// Configure log level
	switch level {
	case "debug":
		log.SetLevel(logrus.DebugLevel)
	case "info":
		log.SetLevel(logrus.InfoLevel)
	case "warn":
		log.SetLevel(logrus.WarnLevel)
	case "error":
		log.SetLevel(logrus.ErrorLevel)
	default:
		log.SetLevel(logrus.InfoLevel) // Default to info level
	}

	// Configure output format
	if format == "json" {
		log.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: "2006-01-02T15:04:05.000Z",
		})
	} else {
		log.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02 15:04:05",
		})
	}

	return &Logger{Logger: log}
}

// Fatal logs a fatal error message with optional structured fields and exits the program
func (l *Logger) Fatal(msg string, args ...interface{}) {
	if len(args) > 0 {
		l.WithFields(argsToFields(args...)).Fatal(msg)
	} else {
		l.Logger.Fatal(msg)
	}
}

// Error logs an error message with optional structured fields
func (l *Logger) Error(msg string, args ...interface{}) {
	if len(args) > 0 {
		l.WithFields(argsToFields(args...)).Error(msg)
	} else {
		l.Logger.Error(msg)
	}
}

// Warn logs a warning message with optional structured fields
func (l *Logger) Warn(msg string, args ...interface{}) {
	if len(args) > 0 {
		l.WithFields(argsToFields(args...)).Warn(msg)
	} else {
		l.Logger.Warn(msg)
	}
}

// Info logs an info message with optional structured fields
func (l *Logger) Info(msg string, args ...interface{}) {
	if len(args) > 0 {
		l.WithFields(argsToFields(args...)).Info(msg)
	} else {
		l.Logger.Info(msg)
	}
}

// Debug logs a debug message with optional structured fields
func (l *Logger) Debug(msg string, args ...interface{}) {
	if len(args) > 0 {
		l.WithFields(argsToFields(args...)).Debug(msg)
	} else {
		l.Logger.Debug(msg)
	}
}

// argsToFields converts key-value pairs to logrus Fields for structured logging
// Usage: logger.Info("message", "key1", value1, "key2", value2)
func argsToFields(args ...interface{}) logrus.Fields {
	fields := make(logrus.Fields)

	// Process arguments in pairs (key, value)
	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			key := fmt.Sprintf("%v", args[i])
			value := args[i+1]
			fields[key] = value
		}
	}

	return fields
}
