package datastore

import (
	"cgl.tideland.biz/applog"
	"code.google.com/p/go.crypto/bcrypt"
	"code.google.com/p/tcgl/redis"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	FEED_DRIVEN_PROFILES = "feeddrivenprofiles"
	ITEMS_NEEDING_IMAGES = "itemsneedingimages"
	FLAGGED_PROFILES     = "flaggedprofiles"
	ITEM_ID              = "itemid"
	MaxInt               = int(^uint(0) >> 1)

	ORDERING_TS = "ts"

	// EVENTED_ITEM_PREFIX = '+'
)

var (
	store             *RedisStore
	ProfileProperties = []string{"name", "feedurl", "bio", "email", "parentpid", "joined", "location", "url", "profileimageurl", "profileimageurlhttps"}
)

type PidType string

func (p PidType) String() string {
	return strings.ToLower(string(p))
}

type ItemIdType string

func (i ItemIdType) String() string {
	return strings.ToLower(string(i))
}

func InitRedisStore(config Config) {
	// Contains all profiles and meta stuff
	profiledb := redis.Connect(redis.Configuration{
		Database: config.Profile.Database,
		Address:  config.Profile.Address,
		PoolSize: config.Profile.PoolSize,
		Timeout:  10 * time.Second,
	})

	// Contains all timelines
	timelinedb := redis.Connect(redis.Configuration{
		Database: config.Timeline.Database,
		Address:  config.Timeline.Address,
		PoolSize: config.Timeline.PoolSize,
		Timeout:  10 * time.Second,
	})

	// Contains all items
	itemdb := redis.Connect(redis.Configuration{
		Database: config.Item.Database,
		Address:  config.Item.Address,
		PoolSize: config.Item.PoolSize,
		Timeout:  10 * time.Second,
	})

	// Contains session information
	sessiondb := redis.Connect(redis.Configuration{
		Database: config.Session.Database,
		Address:  config.Session.Address,
		PoolSize: config.Session.PoolSize,
		Timeout:  10 * time.Second,
	})

	applog.Infof("Connecting to datastores")
	applog.Infof("Profile datastore: %s/%d", config.Profile.Address, config.Profile.Database)
	applog.Infof("Timeline datastore: %s/%d", config.Timeline.Address, config.Timeline.Database)
	applog.Infof("Item datastore: %s/%d", config.Item.Address, config.Item.Database)
	applog.Infof("Session datastore: %s/%d", config.Session.Address, config.Session.Database)

	store = &RedisStore{
		tdb: timelinedb,
		idb: itemdb,
		pdb: profiledb,
		sdb: sessiondb,
	}

}

func NewRedisStore() *RedisStore {
	return store

}

type RedisStore struct {
	tdb *redis.Database
	idb *redis.Database
	pdb *redis.Database
	sdb *redis.Database
}

func itemScore(t time.Time) float64 {
	return float64(t.UnixNano())
}

func followerScore(t time.Time) float64 {
	return float64(t.UnixNano())
}

func profileKey(pid PidType) PidType {
	return PidType(fmt.Sprintf("%s:info", string(pid)))
}

func pidFromKey(key string) PidType {
	if strings.HasSuffix(key, ":info") {
		return PidType(key[:len(key)-5])
	}
	return PidType(key)

}

func feedsKey(pid PidType) string {
	return fmt.Sprintf("%s:feeds", pid)
}

func possiblyKey(pid PidType, ordering string) string {
	return fmt.Sprintf("%s:possibly:%s", pid, ordering)
}

func maybeKey(pid PidType, ordering string) string {
	return fmt.Sprintf("%s:maybe:%s", pid, ordering)
}

func followingKey(pid PidType) string {
	return fmt.Sprintf("%s:following", pid)
}

func followersKey(pid PidType) string {
	return fmt.Sprintf("%s:followers", pid)
}

func itemId(pid PidType, seq int) string {
	return fmt.Sprintf("%s-%d", pid, seq)
}

func ItemKey(itemid ItemIdType) string {
	return fmt.Sprintf("item:%s", itemid)
}

// func EventedItemKey(itemid ItemIdType) string {
// 	return eventedItemKeyFromItemKey(ItemKey(itemid))
// }

// func eventedItemKeyFromItemKey(itemKey string) string {
// 	return fmt.Sprintf("%c%s", EVENTED_ITEM_PREFIX, itemKey)
// }

func suggestedProfileKey(loc string) string {
	return fmt.Sprintf("suggestedprofiles:%s", loc)
}

func sessionKey(num int64) string {
	return fmt.Sprintf("session:%d", num)
}

func oauthSessionKey(key string) string {
	return fmt.Sprintf("oauth:%s", key)
}

func sourcesKey(pid PidType) string {
	return fmt.Sprintf("%s:sources", pid)
}

func (s *RedisStore) Close() {

}

func (s *RedisStore) SuggestedProfiles(loc string) ([]*Profile, error) {
	rs := s.pdb.Command("SMEMBERS", suggestedProfileKey(loc))
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	profiles := make([]*Profile, 0)

	vals := rs.ValuesAsStrings()

	for _, pid := range vals {
		profile, err := s.Profile(PidType(pid))
		if err != nil {
			// TODO: log
		} else {

		}
		profiles = append(profiles, profile)
	}

	return profiles, nil
}

func (s *RedisStore) AddSuggestedProfile(pid PidType, loc string) error {
	rs := s.pdb.Command("SADD", suggestedProfileKey(loc), pid)
	if !rs.IsOK() {
		return rs.Error()
	}

	return nil
}

func (s *RedisStore) RemoveSuggestedProfile(pid PidType, loc string) error {
	rs := s.pdb.Command("SREM", suggestedProfileKey(loc), pid)
	if !rs.IsOK() {
		return rs.Error()
	}

	return nil
}

func (s *RedisStore) ProfileExists(pid PidType) (bool, error) {
	rs := s.pdb.Command("EXISTS", profileKey(pid))
	if !rs.IsOK() {
		return false, rs.Error()
	}

	val, err := rs.ValueAsBool()
	return val, err
}

func (s *RedisStore) Profile(pid PidType) (*Profile, error) {
	rs := s.pdb.Command("HGETALL", profileKey(pid))
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	p := Profile{
		Pid: pid,
	}
	vals := rs.ValuesAsStrings()

	for i := 0; i < len(vals)-1; i += 2 {
		switch vals[i] {
		case "name":
			p.Name = vals[i+1]
		case "bio":
			p.Bio = vals[i+1]
		case "feedurl":
			p.FeedUrl = vals[i+1]
		case "parentpid":
			p.ParentPid = PidType(vals[i+1])
		case "email":
			p.Email = vals[i+1]
		case "location":
			p.Location = vals[i+1]
		case "url":
			p.Url = vals[i+1]
		case "profileimageurl":
			p.ProfileImageUrl = vals[i+1]
		case "profileimageurlhttps":
			p.ProfileImageUrlHttps = vals[i+1]
		case "itemtype":
			p.ItemType = vals[i+1]
		case "joined":
			if v, err := strconv.ParseInt(vals[i+1], 10, 64); err == nil {
				p.Joined = v
			}

		}
	}

	rs = s.pdb.Command("ZCARD", possiblyKey(pid, ORDERING_TS))
	if rs.IsOK() {
		p.PossiblyCount, _ = rs.ValueAsInt()
	}

	rs = s.pdb.Command("ZCARD", maybeKey(pid, ORDERING_TS))
	if rs.IsOK() {
		p.MaybeCount, _ = rs.ValueAsInt()
	}

	rs = s.pdb.Command("ZCARD", followingKey(pid))
	if rs.IsOK() {
		p.FollowingCount, _ = rs.ValueAsInt()
	}

	rs = s.pdb.Command("ZCARD", followersKey(pid))
	if rs.IsOK() {
		p.FollowerCount, _ = rs.ValueAsInt()
	}

	rs = s.pdb.Command("SCARD", feedsKey(pid))
	if rs.IsOK() {
		p.FeedCount, _ = rs.ValueAsInt()
	}

	return &p, nil
}

func (s *RedisStore) BriefProfile(pid PidType) (*BriefProfile, error) {
	rs := s.pdb.Command("HGETALL", profileKey(pid))
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	p := BriefProfile{
		Pid: pid,
	}
	vals := rs.ValuesAsStrings()

	for i := 0; i < len(vals)-1; i += 2 {
		switch vals[i] {
		case "name":
			p.Name = vals[i+1]
		case "profileimageurlhttps":
			p.ProfileImageUrlHttps = vals[i+1]
		}
	}

	return &p, nil
}

func (s *RedisStore) AddProfile(pid PidType, password string, pname string, bio string, feedurl string, parentpid PidType, email string, location string, url string, profileImageUrl string, profileImageUrlHttps string, itemType string) error {
	pwdhash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	joined := time.Now().Unix()
	rs := s.pdb.Command("HMSET", profileKey(pid), "name", pname, "bio", bio, "feedurl", feedurl, "pwdhash", pwdhash, "parentpid", parentpid, "email", email, "joined", joined, "location", location, "url", url, "profileimageurl", profileImageUrl, "profileimageurlhttps", profileImageUrlHttps, "itemtype", itemType)

	if !rs.IsOK() {
		return rs.Error()
	}

	if feedurl != "" {
		rs := s.pdb.Command("SADD", FEED_DRIVEN_PROFILES, pid)
		if !rs.IsOK() {
			return rs.Error()
		}
	}
	if parentpid != "" {
		rs := s.pdb.Command("SADD", feedsKey(parentpid), pid)
		if !rs.IsOK() {
			return rs.Error()
		}
	}

	return nil

}

func (s *RedisStore) UpdateProfile(pid PidType, values map[string]string) error {
	params := make([]interface{}, 0)
	params = append(params, profileKey(pid))
	for k, v := range values {
		params = append(params, k, v)
	}

	if len(params) <= 1 {
		return nil
	}

	rs := s.pdb.Command("HMSET", params...)
	if !rs.IsOK() {
		return rs.Error()
	}

	if feedurl, exists := values["feedurl"]; exists && feedurl != "" {
		// TODO: remove pid if feedurl is empty
		rs := s.pdb.Command("SADD", FEED_DRIVEN_PROFILES, pid)
		if !rs.IsOK() {
			return rs.Error()
		}
	}

	if parentpid, exists := values["parentpid"]; exists && parentpid != "" {
		rs := s.pdb.Command("SADD", feedsKey(PidType(parentpid)), pid)
		if !rs.IsOK() {
			return rs.Error()
		}
	}

	return nil

}

func (s *RedisStore) RemoveProfile(pid PidType) error {

	p, err := s.Profile(pid)
	if err != nil {
		return err
	}

	rs := s.pdb.Command("DEL", possiblyKey(pid, ORDERING_TS))
	if !rs.IsOK() {
		return rs.Error()
	}

	rs = s.pdb.Command("DEL", maybeKey(pid, ORDERING_TS))
	if !rs.IsOK() {
		return rs.Error()
	}

	rs = s.pdb.Command("DEL", profileKey(pid))
	if !rs.IsOK() {
		return rs.Error()
	}

	rs = s.pdb.Command("ZRANGE", followingKey(pid), 0, MaxInt)
	if !rs.IsOK() {
		return rs.Error()
	}

	vals := rs.ValuesAsStrings()
	for _, val := range vals {
		rs = s.pdb.Command("ZREM", followersKey(PidType(val)), pid)
		if !rs.IsOK() {
			return rs.Error()
		}
	}

	rs = s.pdb.Command("DEL", followingKey(pid))
	if !rs.IsOK() {
		return rs.Error()
	}

	rs = s.pdb.Command("SREM", FEED_DRIVEN_PROFILES, pid)
	if !rs.IsOK() {
		return rs.Error()
	}

	if p.ParentPid != "" {
		rs = s.pdb.Command("SREM", feedsKey(p.ParentPid), pid)
		if !rs.IsOK() {
			return rs.Error()
		}
	}

	rs = s.pdb.Command("ZREM", FLAGGED_PROFILES, pid)
	if !rs.IsOK() {
		// OK TO IGNORE
	}

	return nil
}

func (s *RedisStore) FlagProfile(pid PidType) error {

	rs := s.pdb.Command("ZINCRBY", FLAGGED_PROFILES, "1.0", pid)
	if !rs.IsOK() {
		return rs.Error()
	}
	return nil

}

func (s *RedisStore) FlaggedProfiles(start int, count int) ([]*ScoredProfile, error) {
	rs := s.pdb.Command("ZRANGE", FLAGGED_PROFILES, start, start+count, "WITHSCORES")
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	vals := rs.Values()
	profiles := make([]*ScoredProfile, 0)

	for i := 0; i < len(vals); i += 2 {
		pid := vals[i].String()
		score, err := vals[i+1].Float64()
		if err == nil {
			sp := &ScoredProfile{Pid: pid, Score: score}

			profiles = append(profiles, sp)
		}
	}

	return profiles, nil
}

func (s *RedisStore) FeedDrivenProfiles() ([]*Profile, error) {
	rs := s.pdb.Command("SMEMBERS", FEED_DRIVEN_PROFILES)
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	profiles := make([]*Profile, 0)

	vals := rs.ValuesAsStrings()

	for _, pid := range vals {
		profile, err := s.Profile(PidType(pid))
		if err != nil {
			applog.Errorf("Unable to read profile %s from store: %s", pid, err.Error())
			continue
		}
		profiles = append(profiles, profile)
	}

	return profiles, nil
}

func (s *RedisStore) TimelineRange(pid PidType, status string, ts time.Time, before int, after int) ([]*FormattedItem, error) {

	score := itemScore(ts)

	var timelineKey string
	if status == "p" {
		timelineKey = possiblyKey(pid, "ts")
	} else {
		timelineKey = maybeKey(pid, "ts")
	}

	vals := make([]string, 0)
	itemkeys := make([]string, 0)

	if after > 0 {
		// log.Print("ZRANGEBYSCORE", " ", key, " ", score, " ", "+Inf", " ", "WITHSCORES", " ", "LIMIT", " ", 0, " ", after+1)
		rs := s.tdb.Command("ZRANGEBYSCORE", timelineKey, score, "+Inf", "WITHSCORES", "LIMIT", 0, after)
		if !rs.IsOK() {
			return nil, rs.Error()
		}

		vals = rs.ValuesAsStrings()

		itemkeys = make([]string, len(vals))
		for i := 0; i < len(vals); i += 2 {
			itemkeys[len(vals)-i-2] = vals[i]
			itemkeys[len(vals)-i-1] = vals[i+1]

		}
	}

	// Don't include duplicate item
	delim := ""
	if len(vals) > 0 {
		delim = "("
	}
	formattedScore := fmt.Sprintf("%s%f", delim, score)

	// log.Print("ZREVRANGEBYSCORE", " ", key, " ", formattedScore, " ", "-Inf", " ", "WITHSCORES", " ", "LIMIT", " ", 0, " ", before)
	rs := s.tdb.Command("ZREVRANGEBYSCORE", timelineKey, formattedScore, "-Inf", "WITHSCORES", "LIMIT", 0, before+1)
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	vals = rs.ValuesAsStrings()
	for i := 0; i < len(vals); i += 2 {
		itemkeys = append(itemkeys, vals[i])
		itemkeys = append(itemkeys, vals[i+1])
	}

	items := make([]*FormattedItem, 0)

	for ik := 0; ik < len(itemkeys); ik += 2 {

		key := itemkeys[ik]
		score := itemkeys[ik+1]

		// if key[0] == EVENTED_ITEM_PREFIX {
		// 	key = key[1:]
		// }

		rs = s.idb.Command("GET", key)
		if !rs.IsOK() {
			applog.Errorf("Could not get key %s from db: %s", key, rs.Error().Error())
			continue
		}

		item_infos := rs.ValuesAsStrings()
		for _, item_info := range item_infos {
			item := &Item{}

			_ = json.Unmarshal([]byte(item_info), item)

			f, err := strconv.ParseFloat(score, 64)
			if err != nil {
				applog.Errorf("Could not parse score from db as float: %s", err.Error())
				continue
			}

			ts := int64(f)

			fitem, err := s.FormatItem(item, ts, pid)
			if err != nil {
				applog.Errorf("Could not format item: %s", err.Error())
				continue
			}
			items = append(items, fitem)
		}
	}
	return items, nil
}

// Gets an item as it appears in a timeline along with any associated event
func (s *RedisStore) ItemInTimeline(item *Item, pid PidType, status string) ([]*FormattedItem, error) {
	items := make([]*FormattedItem, 0)

	var timelineKey string
	if status == "p" {
		timelineKey = possiblyKey(pid, "ts")
	} else {
		timelineKey = maybeKey(pid, "ts")
	}

	// if item.IsEvent() {
	// 	ets := s.ItemScore(item.EventKey(), timelineKey)
	// 	fevent, err := s.FormatItem(item, ets, pid)
	// 	if err != nil {
	// 		return items, err
	// 	}
	// 	items = append(items, fevent)
	// }

	ts := s.ItemScore(item.Key(), timelineKey)

	fitem, err := s.FormatItem(item, ts, pid)
	if err != nil {
		return items, err
	}

	items = append(items, fitem)

	return items, nil
}

func (s *RedisStore) FormatItem(item *Item, ts int64, pid PidType) (*FormattedItem, error) {
	source := PidType("")
	sourcesKey := sourcesKey(pid)
	rs := s.tdb.Command("HGET", sourcesKey, item.Key())
	if !rs.IsOK() {
		// Ignore
	} else {
		source = PidType(rs.ValueAsString())
	}
	fitem := &FormattedItem{Item: *item, Ts: ts}
	fitem.Added = item.Added / 1000000000
	fitem.Event = item.Event / 1000000000

	aprofile, err := s.BriefProfile(item.Pid)
	if err != nil {
		return nil, err
	}
	fitem.Author = aprofile

	if source != PidType("") && source != item.Pid && source != pid {
		sprofile, err := s.BriefProfile(source)
		if err != nil {
			return nil, err
		}
		fitem.Via = sprofile
	}
	return fitem, nil

}

func (s *RedisStore) ItemScore(itemKey string, timelineKey string) int64 {
	rs := s.tdb.Command("ZSCORE", timelineKey, itemKey)
	if !rs.IsOK() {
		// Ignore
	} else {
		f, err := strconv.ParseFloat(rs.ValueAsString(), 64)
		if err != nil {
			applog.Errorf("Could not parse score from db as float: %s", err.Error())
			return 0
		}

		return int64(f)
	}

	return 0
}

// Gets a raw item
func (s *RedisStore) Item(id ItemIdType) (*Item, error) {
	return s.ItemByKey(ItemKey(id))
}

// Gets a raw item
func (s *RedisStore) ItemByKey(itemKey string) (*Item, error) {
	rs := s.idb.Command("GET", itemKey)
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	item := &Item{}
	_ = json.Unmarshal([]byte(rs.Value()), item)

	return item, nil
}

func (s *RedisStore) AddItem(pid PidType, ets time.Time, text string, link string, image string, itemid ItemIdType, media string, duration int) (ItemIdType, error) {

	if itemid == "" {

		hasher := md5.New()
		io.WriteString(hasher, string(pid))
		io.WriteString(hasher, text)
		io.WriteString(hasher, link)
		io.WriteString(hasher, ets.String())
		itemid = ItemIdType(fmt.Sprintf("%x", hasher.Sum(nil)))
	}

	// eventedItemKey := EventedItemKey(itemid)
	itemKey := ItemKey(itemid)
	if exists, _ := s.ItemExists(itemid); exists {
		applog.Debugf("Attempted to add item %s but it already exists", itemid)
		// promote it instead
		return itemid, s.Promote(pid, itemid)
	}

	etsnano := FakeEventPrecision(ets)

	tsnano := time.Now().UnixNano()
	item := &Item{

		Id:       itemid,
		Text:     text,
		Link:     link,
		Pid:      pid,
		Added:    tsnano,
		Event:    etsnano,
		Image:    image,
		Media:    media,
		Duration: duration,
	}

	itemKey, err := s.SaveItem(item, 0)
	if err != nil {
		return "", err
	}

	scheduledTime := item.DefaultScheduledTime()

	rs := s.tdb.Command("ZADD", maybeKey(pid, ORDERING_TS), scheduledTime, itemKey)
	if !rs.IsOK() {
		return "", rs.Error()
	}

	// if item.IsEvent() {
	// 	rs = s.tdb.Command("ZADD", maybeKey(pid, ORDERING_TS), item.Event, eventedItemKey)
	// 	if !rs.IsOK() {
	// 		return "", rs.Error()
	// 	}
	// }

	s.AddItemToFollowerTimelines(pid, scheduledTime, item)

	if item.Link != "" && item.Image == "" {
		rs := s.pdb.Command("SADD", ITEMS_NEEDING_IMAGES, itemid)
		if !rs.IsOK() {
			return "", rs.Error()
		}
	}

	return itemid, nil
}

// lifetime is in seconds, 0 means permanent
func (s *RedisStore) SaveItem(item *Item, lifetime int) (string, error) {

	item.Sanitize()

	itemKey := ItemKey(item.Id)

	json, err := json.Marshal(item)
	if err != nil {
		return "", err
	}

	rs := s.idb.Command("SET", itemKey, json)
	if !rs.IsOK() {
		return "", rs.Error()
	}

	if lifetime > 0 {
		rs := s.pdb.Command("EXPIRE", itemKey, lifetime)
		if !rs.IsOK() {
			applog.Errorf("Could not set expiry for temporary item %s: %s", itemKey, rs.Error().Error())
		}

	}

	return itemKey, nil

}

func (s *RedisStore) ItemExists(id ItemIdType) (bool, error) {
	rs := s.idb.Command("EXISTS", ItemKey(id))
	if !rs.IsOK() {
		return false, rs.Error()
	}

	val, err := rs.ValueAsBool()
	return val, err
}

func (s *RedisStore) UpdateItem(item *Item) error {
	itemKey := ItemKey(item.Id)

	json, err := json.Marshal(item)
	if err != nil {
		return err
	}

	rs := s.idb.Command("SET", itemKey, json)
	if !rs.IsOK() {
		return rs.Error()
	}
	return nil

}

func (s *RedisStore) DeleteMaybeItems(pid PidType) error {
	rs := s.tdb.Command("DEL", maybeKey(pid, ORDERING_TS))
	if !rs.IsOK() {
		return rs.Error()
	}

	return nil
}

func (s *RedisStore) ResetAll() error {
	rs := s.pdb.Command("FLUSHDB")
	if !rs.IsOK() {
		return rs.Error()
	}

	rs = s.tdb.Command("FLUSHDB")
	if !rs.IsOK() {
		return rs.Error()
	}

	rs = s.idb.Command("FLUSHDB")
	if !rs.IsOK() {
		return rs.Error()
	}

	rs = s.sdb.Command("FLUSHDB")
	if !rs.IsOK() {
		return rs.Error()
	}

	return nil
}

// Make pid follow followpid
func (s *RedisStore) Follow(pid PidType, followpid PidType) error {
	score := followerScore(time.Now())

	rs := s.pdb.Command("ZADD", followingKey(pid), score, followpid)
	if !rs.IsOK() {
		return rs.Error()
	}

	rs = s.pdb.Command("ZADD", followersKey(followpid), score, pid)
	if !rs.IsOK() {
		return rs.Error()
	}

	// Copy all of followpid's items into pid's timeline
	// Slow but accurate method

	sourceTimelineKey := maybeKey(followpid, ORDERING_TS)
	rs = s.tdb.Command("ZRANGEBYSCORE", sourceTimelineKey, 0, "+Inf", "WITHSCORES")
	if !rs.IsOK() {
		return rs.Error()
	}

	// 	get itemkey followed by score
	vals := rs.ValuesAsStrings()
	for ik := 0; ik < len(vals); ik += 2 {

		itemKey := vals[ik]
		score := vals[ik+1]

		f, err := strconv.ParseFloat(score, 64)
		if err != nil {
			applog.Errorf("Could not parse score from db as float: %s", err.Error())
			continue
		}

		ts := int64(f)
		s.AddItemToTimeline(pid, followpid, ts, itemKey)
	}
	return nil
}

// Make pid stop following followpid
func (s *RedisStore) Unfollow(pid PidType, followpid PidType) error {

	rs := s.pdb.Command("ZREM", followingKey(pid), followpid)
	if !rs.IsOK() {
		return rs.Error()
	}

	rs = s.pdb.Command("ZREM", followersKey(followpid), pid)
	if !rs.IsOK() {
		return rs.Error()
	}

	// Remove all of followpid's items from pid's timeline
	// Slow but accurate method

	sourceTimelineKey := maybeKey(followpid, ORDERING_TS)
	applog.Debugf("ZRANGEBYSCORE %s 0 +Inf", sourceTimelineKey)
	rs = s.tdb.Command("ZRANGEBYSCORE", sourceTimelineKey, 0, "+Inf")
	if !rs.IsOK() {
		return rs.Error()
	}

	// 	get itemkey
	for _, itemKey := range rs.ValuesAsStrings() {
		applog.Debugf("Found itemkey %s", itemKey)
		s.RemoveItemFromTimeline(pid, followpid, itemKey)
	}
	return nil
}

func (s *RedisStore) Promote(pid PidType, id ItemIdType) error {

	itemKey := ItemKey(id)

	// rs := s.tdb.Command("ZREM", poss_key, itemKey)
	// if !rs.IsOK() {
	// 	// Ignore error
	// }

	// rs = s.tdb.Command("ZREM", poss_key, eventedItemKey)
	// if !rs.IsOK() {
	// 	// Ignore error
	// }

	// Ensure the item is persisted (in case it's a temporary search item)
	rs := s.tdb.Command("PERSIST", itemKey)
	if !rs.IsOK() {
		return rs.Error()
	}

	item, err := s.Item(id)
	if err != nil {
		return err
	}

	scheduledTime := time.Now().UnixNano()
	if item.IsEvent() {
		scheduledTime = item.Event
	}

	maybe_key := maybeKey(pid, ORDERING_TS)

	rs = s.tdb.Command("ZADD", maybe_key, scheduledTime, itemKey)
	if !rs.IsOK() {
		return rs.Error()
	}

	// if item.Event > 0 {
	// 	eventedItemKey := EventedItemKey(id)
	// 	rs = s.tdb.Command("ZADD", maybe_key, item.Event, eventedItemKey)
	// 	if !rs.IsOK() {
	// 		return rs.Error()
	// 	}
	// }

	s.AddItemToFollowerTimelines(pid, scheduledTime, item)

	return nil

}

func (s *RedisStore) Demote(pid PidType, id ItemIdType) error {

	// TODO: abort if item is not in possibly timeline
	itemKey := ItemKey(id)

	maybe_key := maybeKey(pid, ORDERING_TS)

	rs := s.tdb.Command("ZREM", maybe_key, itemKey)
	if !rs.IsOK() {
		return rs.Error()
	}

	// rs = s.tdb.Command("ZREM", maybe_key, eventedItemKey)
	// if !rs.IsOK() {
	// 	return rs.Error()
	// }

	// TODO: don't add them here if they were manually added by the user

	// tsnano := time.Now().UnixNano()
	// poss_key := possiblyKey(pid, ORDERING_TS)

	// rs = s.tdb.Command("ZADD", poss_key, tsnano, itemKey)
	// if !rs.IsOK() {
	// 	return rs.Error()
	// }

	// item, err := s.Item(id)
	// if err != nil {
	// 	return err
	// }

	// if item.Event > 0 {
	// 	rs = s.tdb.Command("ZADD", poss_key, item.Event, eventedItemKey)
	// 	if !rs.IsOK() {
	// 		return rs.Error()
	// 	}
	// }
	s.RemoveItemFromFollowerTimelines(pid, itemKey)

	return nil
}

func (s *RedisStore) Followers(pid PidType, count int, start int) ([]*FollowingProfile, error) {
	rs := s.pdb.Command("ZRANGE", followersKey(pid), start, start+count)
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	profiles := make([]*FollowingProfile, 0)

	vals := rs.ValuesAsStrings()

	for _, fpid := range vals {
		profile, err := s.Profile(PidType(fpid))
		if err != nil {
			applog.Errorf("Could not retrieve profile for %s: %s", fpid, err.Error())
		} else {
			follows, _ := s.Follows(PidType(fpid), pid)
			fprofile := &FollowingProfile{Profile: *profile, Reciprocal: follows}
			profiles = append(profiles, fprofile)
		}
	}

	return profiles, nil

}

func (s *RedisStore) Following(pid PidType, count int, start int) ([]*FollowingProfile, error) {
	rs := s.pdb.Command("ZRANGE", followingKey(pid), start, start+count)
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	profiles := make([]*FollowingProfile, 0)

	vals := rs.ValuesAsStrings()

	for _, fpid := range vals {
		profile, err := s.Profile(PidType(fpid))
		if err != nil {
			applog.Errorf("Could not retrieve profile for %s: %s", fpid, err.Error())
		} else {
			follows, _ := s.Follows(pid, PidType(fpid))
			fprofile := &FollowingProfile{Profile: *profile, Reciprocal: follows}
			profiles = append(profiles, fprofile)
		}

	}

	return profiles, nil

}

// Returns whether follower follows pid
func (s *RedisStore) Follows(pid PidType, follower PidType) (bool, error) {
	rs := s.pdb.Command("ZSCORE", followersKey(pid), follower)
	if !rs.IsOK() {
		if rs.Error().Error() != "redis: key not found" {
			applog.Errorf("Could not get score for %s in %s: %s", follower, followersKey(pid), rs.Error().Error())
			return false, rs.Error()
		}
		return false, nil
	}

	return true, nil
}

func (s *RedisStore) VerifyPassword(pid PidType, password string) (bool, error) {
	rs := s.pdb.Command("HGET", profileKey(pid), "pwdhash")
	if !rs.IsOK() {
		return false, rs.Error()
	}

	pwdhash := rs.ValueAsString()

	err := bcrypt.CompareHashAndPassword([]byte(pwdhash), []byte(password))
	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *RedisStore) SessionId(pid PidType) (int64, error) {
	sessionId := rand.Int63()
	rs := s.sdb.Command("SET", sessionKey(sessionId), pid)
	if !rs.IsOK() {
		return 0, rs.Error()
	}

	return sessionId, nil
}

func (s *RedisStore) ValidSession(pid PidType, sessionId int64) (bool, error) {
	rs := s.sdb.Command("GET", sessionKey(sessionId))
	if !rs.IsOK() {
		return false, rs.Error()
	}

	return (PidType(rs.ValueAsString()) == pid), nil

}

func (s *RedisStore) SetOauthSessionData(key string, data string) error {

	rs := s.sdb.Command("SET", oauthSessionKey(key), data)
	if !rs.IsOK() {
		return rs.Error()
	}
	rs = s.pdb.Command("EXPIRE", oauthSessionKey(key), 600)
	if !rs.IsOK() {
		return rs.Error()
	}

	return nil
}

func (s *RedisStore) GetOauthSessionData(key string) (string, error) {
	rs := s.sdb.Command("GET", oauthSessionKey(key))
	if !rs.IsOK() {
		return "", rs.Error()
	}

	return rs.ValueAsString(), nil

}

func (s *RedisStore) Feeds(pid PidType) ([]*Profile, error) {
	rs := s.pdb.Command("SMEMBERS", feedsKey(pid))
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	feeds := make([]*Profile, 0)

	vals := rs.ValuesAsStrings()

	for _, fid := range vals {
		feed, err := s.Profile(PidType(fid))
		if err != nil {
			// TODO: log
		} else {

		}
		feeds = append(feeds, feed)
	}

	return feeds, nil

}

func (s *RedisStore) GrabItemsNeedingImages(max int) ([]*Item, error) {
	items := make([]*Item, 0)

	if keyExists(s.pdb, ITEMS_NEEDING_IMAGES) {

		for i := 0; i < max; i++ {

			rs := s.pdb.Command("SRANDMEMBER", ITEMS_NEEDING_IMAGES)
			if !rs.IsOK() {
				return items, rs.Error()
			}

			itemid := ItemIdType(rs.ValueAsString())

			s.pdb.Command("SREM", ITEMS_NEEDING_IMAGES, itemid)
			item, err := s.Item(itemid)
			if err == nil {
				items = append(items, item)
			}
		}
	}
	return items, nil
}

func (s *RedisStore) FindProfilesBySubstring(srch string) ([]*Profile, error) {

	profiles := make([]*Profile, 0)

	// Full wildcard is an exception
	if srch == "*" {
		srch = ""
	} else {
		// Bail if the search contains non alphanumerics or @
		allgood, _ := regexp.MatchString("^[a-zA-Z0-9@]+$", srch)
		if !allgood {

			applog.Errorf("Invalid search string supplied: %s", srch)
			return profiles, nil
		}
	}

	searchKey := fmt.Sprintf("*%s*:info", srch)
	rs := s.pdb.Command("KEYS", searchKey)
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	vals := rs.ValuesAsStrings()

	for _, key := range vals {
		profile, err := s.Profile(pidFromKey(key))
		if err != nil {
			// TODO: log
		} else {
			profiles = append(profiles, profile)
		}
	}

	return profiles, nil

}

func dumpKeys(db *redis.Database, pattern string) {
	rs := db.Command("KEYS", pattern)
	if !rs.IsOK() {
		applog.Errorf("Error dumping keys: %s", rs.Error().Error())
		return
	}

	for _, v := range rs.ValuesAsStrings() {
		log.Printf("Key: ->%s<-", v)
	}
}

func keyExists(db *redis.Database, key string) bool {
	rs := db.Command("EXISTS", key)
	if !rs.IsOK() {
		applog.Errorf("Unexpected error checking if key exists: %s", rs.Error().Error())
		return false
	}

	val, _ := rs.ValueAsBool()
	return val
}

func (s *RedisStore) AddItemToFollowerTimelines(pid PidType, scheduledTime int64, item *Item) error {

	rs := s.pdb.Command("ZRANGE", followersKey(pid), 0, MaxInt)
	if !rs.IsOK() {
		applog.Errorf("Could not list followers for pid %s: %s", pid, rs.Error().Error())
		return rs.Error()
	}

	for _, followerpid := range rs.ValuesAsStrings() {
		// Don't add circular references
		if PidType(followerpid) != item.Pid {
			s.AddItemToTimeline(PidType(followerpid), pid, scheduledTime, item.Key())
			// if item.IsEvent() {
			// 	s.AddItemToTimeline(PidType(followerpid), pid, item.Event, item.EventKey())
			// }
		}
	}

	return nil
}

func (s *RedisStore) AddItemToTimeline(pid PidType, source PidType, ts int64, itemKey string) error {
	applog.Debugf("Adding item %s to timeline for %s with source %s", itemKey, pid, source)
	timelineKey := possiblyKey(pid, ORDERING_TS)

	rs := s.tdb.Command("ZSCORE", timelineKey, itemKey)
	if !rs.IsOK() {
		if rs.Error().Error() != "redis: key not found" {
			applog.Errorf("Could not get score for %s in %s: %s", itemKey, timelineKey, rs.Error().Error())
			return rs.Error()
		}

		rs = s.tdb.Command("ZADD", timelineKey, ts, itemKey)
		if !rs.IsOK() {
			applog.Errorf("Could not add item %s to timeline %s: %s", itemKey, timelineKey, rs.Error().Error())
			return rs.Error()
		}

		// Remember the source of the item
		sourcesKey := sourcesKey(pid)
		rs = s.tdb.Command("HSET", sourcesKey, itemKey, source)
		if !rs.IsOK() {
			applog.Errorf("Could not set source for %s in %s: %s", itemKey, sourcesKey, rs.Error().Error())
			return rs.Error()
		}

	}

	return nil
}

func (s *RedisStore) RemoveItemFromFollowerTimelines(pid PidType, itemKey string) error {

	rs := s.pdb.Command("ZRANGE", followersKey(pid), 0, MaxInt)
	if !rs.IsOK() {
		applog.Errorf("Could not list followers for pid %s: %s", pid, rs.Error().Error())
		return rs.Error()
	}

	for _, followerpid := range rs.ValuesAsStrings() {
		s.RemoveItemFromTimeline(PidType(followerpid), pid, itemKey)
		// s.RemoveItemFromTimeline(PidType(followerpid), pid, eventedItemKeyFromItemKey(itemKey))
		// if item.IsEvent() {
		// 	s.RemoveItemFromTimeline(followerpid, pid, item.Event, item.EventKey())
		// }
	}

	return nil
}

func (s *RedisStore) RemoveItemFromTimeline(pid PidType, source PidType, itemKey string) error {
	applog.Debugf("Removing item %s from timeline for %s", itemKey, pid)
	sourcesKey := sourcesKey(pid)

	// Only remove if source is same as the source recorded for this timeline
	rs := s.tdb.Command("HGET", sourcesKey, itemKey)
	if !rs.IsOK() {
		applog.Errorf("Could not get a source for item %s for pid %s: %s", itemKey, pid, rs.Error().Error())
		return rs.Error()
	}

	if source != PidType(rs.ValueAsString()) {
		// Don't remove the item because it came from a different source
		return nil
	}

	timelineKey := possiblyKey(pid, ORDERING_TS)

	rs = s.tdb.Command("ZREM", timelineKey, itemKey)
	if !rs.IsOK() {
		return rs.Error()
	}

	rs = s.tdb.Command("HDEL", sourcesKey, itemKey)
	if !rs.IsOK() {
		return rs.Error()
	}

	return nil

}

// Fakes some nano second precision for events for unique ordering
func FakeEventPrecision(ets time.Time) int64 {

	etsnano := ets.UnixNano()

	if etsnano > 0 {
		// Fake the precision for event time
		tsnano := time.Now().UnixNano()
		nanos := tsnano - 1000000000*(tsnano/1000000000)
		etsnano = ets.Unix()*1000000000 + nanos
	}

	return etsnano
}
