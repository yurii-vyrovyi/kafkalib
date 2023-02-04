package dummylogger

import "fmt"

type Logger struct{}

func (l *Logger) Debug(args ...interface{}) {
	fmt.Println("[DEBUG]", fmt.Sprint(args...))
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	fmt.Println("[DEBUG]", fmt.Sprintf(format, args...))
}

func (l *Logger) Info(args ...interface{}) {
	fmt.Println("[INFO]", fmt.Sprint(args...))
}

func (l *Logger) Infof(format string, args ...interface{}) {
	fmt.Println("[INFO]", fmt.Sprintf(format, args...))
}

func (l *Logger) Error(args ...interface{}) {
	fmt.Println("[ERROR]", fmt.Sprint(args...))
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	fmt.Println("[ERROR]", fmt.Sprintf(format, args...))
}
