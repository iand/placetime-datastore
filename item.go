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
	Id    ItemIdType `json:"id"`
	Added int64      `json:"added"`
	Event int64      `json:"event"`
	Pid   PidType    `json:"pid"`
	PName string     `json:"name,omitempty"`
	Text  string     `json:"text"`
	Link  string     `json:"link"`
	Media string     `json:"media"`
	Image string     `json:"image"`
}

type FormattedItem struct {
	Item
	Ts     int64  `json:"ts"`
	Source string `json:"source"`
}

func NewFormattedItem(item *Item, ts int64, source string) *FormattedItem {
	fitem := &FormattedItem{Item: *item, Ts: ts, Source: source}
	fitem.Added = item.Added / 1000000000
	fitem.Event = item.Event / 1000000000
	return fitem
}

func (i *Item) String() string {
	return fmt.Sprintf("Title: %sLink: %s", i.Text, i.Link)
}

func (i *Item) IsEvent() bool {
	return i.Event > 0
}

func (i *Item) Key() string {
	return ItemKey(i.Id)
}

func (i *Item) EventKey() string {
	return EventedItemKey(i.Id)
}
