package datastore

import (
	"fmt"
	"time"
)

type TimelineRange struct {
	Pid    string    `json:"pid"`
	PName  string    `json:"pname,omitempty"`
	Tstart time.Time `json:"tstart"`
	Tend   time.Time `json:"tend"`
	Items  []*Item   `json:"items"`
}

type Item struct {
	Id    string `json:"id"`
	Added int64  `json:"added"`
	Event int64  `json:"event"`
	Pid   string `json:"pid"`
	PName string `json:"pname,omitempty"`
	Text  string `json:"text"`
	Link  string `json:"link"`
	Media string `json:"media"`
	Image string `json:"image"`
}

type FormattedItem struct {
	Id     string `json:"id"`
	Ts     int64  `json:"ts"`
	Added  int64  `json:"added"`
	Event  int64  `json:"event"`
	Pid    string `json:"pid"`
	PName  string `json:"pname,omitempty"`
	Text   string `json:"text"`
	Link   string `json:"link"`
	Media  string `json:"media"`
	Image  string `json:"image"`
	Source string `json:"source"`
}

func (i *Item) String() string {
	return fmt.Sprintf("Title: %sLink: %s", i.Text, i.Link)
}

func (i *Item) IsEvent() bool {
	return i.Event > 0
}

func (i *Item) Key() string {
	return itemKey(i.Id)
}

func (i *Item) EventKey() string {
	return eventedItemKey(i.Id)
}
