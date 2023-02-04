package kafkalib

type Error string

func (e Error) Error() string {
	return string(e)
}

const (
	ErrNoSchema       = Error("no schema")
	ErrInvalidMessage = Error("invalid message")
)
