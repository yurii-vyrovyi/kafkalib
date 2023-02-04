package kafkalib

import (
	"fmt"

	logger "github.com/sirupsen/logrus"
)

type Logger interface {
	Debug(...interface{})
	Debugf(string, ...interface{})
	Info(...interface{})
	Infof(string, ...interface{})
	Error(...interface{})
	Errorf(string, ...interface{})
}

type DefaultLogger struct{}

func (l DefaultLogger) Debug(args ...interface{})                 { logger.Debug(args...) }
func (l DefaultLogger) Debugf(format string, args ...interface{}) { logger.Debugf(format, args...) }
func (l DefaultLogger) Info(args ...interface{})                  { logger.Info(args...) }
func (l DefaultLogger) Infof(format string, args ...interface{})  { logger.Infof(format, args...) }
func (l DefaultLogger) Error(args ...interface{})                 { logger.Error(args...) }
func (l DefaultLogger) Errorf(format string, args ...interface{}) { logger.Errorf(format, args...) }

type TaggedLogger struct {
	l Logger
}

func NewTaggedLogger(l Logger) *TaggedLogger {
	if l == nil {
		return nil
	}

	return &TaggedLogger{l: l}
}

const KafkalibLogPrefix = "[kafkalib] "

func (l TaggedLogger) Debug(args ...interface{}) {
	l.l.Debug(KafkalibLogPrefix, fmt.Sprint(args...))
}

func (l TaggedLogger) Debugf(format string, args ...interface{}) {
	l.l.Debug(KafkalibLogPrefix, fmt.Sprintf(format, args...))
}

func (l TaggedLogger) Info(args ...interface{}) {
	l.l.Info(KafkalibLogPrefix, fmt.Sprint(args...))
}

func (l TaggedLogger) Infof(format string, args ...interface{}) {
	l.l.Info(KafkalibLogPrefix, fmt.Sprintf(format, args...))
}

func (l TaggedLogger) Error(args ...interface{}) {
	l.l.Error(KafkalibLogPrefix, fmt.Sprint(args...))
}

func (l TaggedLogger) Errorf(format string, args ...interface{}) {
	l.l.Error(KafkalibLogPrefix, fmt.Sprintf(format, args...))
}
