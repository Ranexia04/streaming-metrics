package flow

import (
	"encoding/json"
	"time"

	"github.com/sirupsen/logrus"
)

type Event struct {
	Time     time.Time
	TimeStr  string `json:"strtTm"`
	Duration int64  `json:"drtn"`

	Type       string `json:"tp"`
	System     string `json:"systm"`
	Domain     string `json:"dmn"`
	Component  string `json:"cmpnnt"`
	Function   string `json:"nm"`
	Operation  string `json:"oprtn"`
	Hostname   string `json:"hstnm"`
	IsError    bool   `json:"isErr"`
	StatusCode string `json:"sCd"`
	NativeCode string `json:"nCd"`
}

func NewEvent(msg []byte) *Event {
	var event Event
	if err := json.Unmarshal(msg, &event); err != nil {
		logrus.Errorf("msg is not json: %+v", err)
		return nil
	}

	parsedTime, err := time.Parse(time.RFC3339, event.TimeStr)
	if err != nil {
		logrus.Errorf("time is not RFC3339: %+v", err)
		return nil
	}
	event.Time = parsedTime

	return &event
}
