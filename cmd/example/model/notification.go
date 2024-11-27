package model

import (
	"errors"
	"strings"

	"github.com/hallgren/eventsourcing"
)

type NotificationState int

const (
	Unread NotificationState = iota
	Read
)

type NotificationServity int

const (
	NotificationServityInfo NotificationServity = iota
	NotificationServityWarning
	NotificationServityError
)

func (s NotificationServity) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

func (s *NotificationServity) UnmarshalJSON(data []byte) error {
	servity, err := NotificationServityFromString(string(data[1 : len(data)-1]))
	if err != nil {
		return err
	}
	*s = servity
	return nil
}

func (s NotificationServity) String() string {
	switch s {
	case NotificationServityInfo:
		return "INFO"
	case NotificationServityWarning:
		return "WARNING"
	case NotificationServityError:
		return "ERROR"
	default:
		panic("unknown servity")
	}
}

func NotificationServityFromString(s string) (NotificationServity, error) {
	s = strings.ToUpper(s)
	switch s {
	case "INFO":
		return NotificationServityInfo, nil
	case "WARNING":
		return NotificationServityWarning, nil
	case "ERROR":
		return NotificationServityError, nil
	default:
		return 0, errors.New("unknown servity")
	}
}

type Notification struct {
	Title   string
	Body    string
	State   NotificationState
	Hidden  bool
	Servity NotificationServity
}

type NotificationAggregate struct {
	eventsourcing.AggregateRoot
	Notification
}

func (n *NotificationAggregate) Register(r eventsourcing.RegisterFunc) {
	r(
		&NotificationCreated{},
		&NotificationRead{},
		&NotificationHidden{},
	)
}

func (n *NotificationAggregate) Transition(event eventsourcing.Event) {
	switch e := event.Data().(type) {
	case *NotificationCreated:
		n.Title = e.Title
		n.Body = e.Body
		n.Servity = e.Servity
		n.State = Unread
		n.Hidden = false
	case *NotificationRead:
		n.State = Read
	case *NotificationHidden:
		n.Hidden = true
	}
}

// Events
type NotificationCreated struct {
	Title    string
	Body     string
	Servity  NotificationServity
	Metadata map[string][]string
}

type NotificationRead struct {
}

type NotificationHidden struct {
}

func CreateNotification(
	title string,
	body string,
	servity NotificationServity,
	metadata map[string][]string,
) (*NotificationAggregate, error) {
	notification := NotificationAggregate{}
	notification.TrackChange(&notification, &NotificationCreated{
		Title:    title,
		Body:     body,
		Servity:  servity,
		Metadata: metadata,
	})
	return &notification, nil
}
