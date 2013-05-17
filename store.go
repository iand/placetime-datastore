package datastore

import (
	"cgl.tideland.biz/applog"
	"code.google.com/p/go.crypto/bcrypt"
	"code.google.com/p/tcgl/redis"
	"encoding/json"
	"fmt"
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

	EVENTED_ITEM_PREFIX = '+'
)

var (
	store             *RedisStore
	ProfileProperties = []string{"name", "feedurl", "bio", "email", "parentpid"}
)

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

func eventedItemScore(t time.Time, ets time.Time) float64 {

	nanos := t.UnixNano() - 1000000000*(t.UnixNano()/1000000000)
	return float64(ets.Unix()*1000000000 + nanos)
}

func followerScore(t time.Time) float64 {
	return float64(t.UnixNano())
}

func profileKey(pid string) string {
	return fmt.Sprintf("%s:info", pid)
}

func pidFromKey(key string) string {
	if strings.HasSuffix(key, ":info") {
		return key[:len(key)-5]
	}
	return key

}

func feedsKey(pid string) string {
	return fmt.Sprintf("%s:feeds", pid)
}

func possiblyKey(pid string, ordering string) string {
	return fmt.Sprintf("%s:possibly:%s", pid, ordering)
}

func maybeKey(pid string, ordering string) string {
	return fmt.Sprintf("%s:maybe:%s", pid, ordering)
}

func followingKey(pid string) string {
	return fmt.Sprintf("%s:following", pid)
}

func followersKey(pid string) string {
	return fmt.Sprintf("%s:followers", pid)
}

func itemSeqKey(pid string) string {
	return fmt.Sprintf("%s:itemseq", pid)
}

func itemId(pid string, seq int) string {
	return fmt.Sprintf("%s-%d", pid, seq)
}

func itemKey(itemid string) string {
	return fmt.Sprintf("item:%s", itemid)
}

func eventedItemKey(itemid string) string {
	return eventedItemKeyFromItemKey(itemKey(itemid))
}

func eventedItemKeyFromItemKey(itemKey string) string {
	return fmt.Sprintf("%c%s", EVENTED_ITEM_PREFIX, itemKey)
}

func suggestedProfileKey(loc string) string {
	return fmt.Sprintf("suggestedprofiles:%s", loc)
}

func sessionKey(num int64) string {
	return fmt.Sprintf("session:%d", num)
}

func sourcesKey(pid string) string {
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
		profile, err := s.Profile(pid)
		if err != nil {
			// TODO: log
		} else {

		}
		profiles = append(profiles, profile)
	}

	return profiles, nil
}

func (s *RedisStore) AddSuggestedProfile(pid string, loc string) error {
	rs := s.pdb.Command("SADD", suggestedProfileKey(loc), pid)
	if !rs.IsOK() {
		return rs.Error()
	}

	return nil
}

func (s *RedisStore) RemoveSuggestedProfile(pid string, loc string) error {
	rs := s.pdb.Command("SREM", suggestedProfileKey(loc), pid)
	if !rs.IsOK() {
		return rs.Error()
	}

	return nil
}

func (s *RedisStore) ProfileExists(pid string) (bool, error) {
	rs := s.pdb.Command("EXISTS", profileKey(pid))
	if !rs.IsOK() {
		return false, rs.Error()
	}

	val, err := rs.ValueAsBool()
	return val, err
}

func (s *RedisStore) Profile(pid string) (*Profile, error) {
	rs := s.pdb.Command("HMGET", profileKey(pid), "name", "bio", "feedurl", "parentpid", "email")
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	vals := rs.ValuesAsStrings()
	p := Profile{
		Pid:       pid,
		Name:      vals[0],
		Bio:       vals[1],
		FeedUrl:   vals[2],
		ParentPid: vals[3],
		Email:     vals[4],
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

func (s *RedisStore) AddProfile(pid string, password string, pname string, bio string, feedurl string, parentpid string, email string) error {
	pwdhash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	rs := s.pdb.Command("HMSET", profileKey(pid), "name", pname, "bio", bio, "feedurl", feedurl, "pwdhash", pwdhash, "parentpid", parentpid, "email", email)

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

func (s *RedisStore) UpdateProfile(pid string, values map[string]string) error {
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
		rs := s.pdb.Command("SADD", feedsKey(parentpid), pid)
		if !rs.IsOK() {
			return rs.Error()
		}
	}

	return nil

}

func (s *RedisStore) RemoveProfile(pid string) error {

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
		rs = s.pdb.Command("ZREM", followersKey(val), pid)
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

func (s *RedisStore) FlagProfile(pid string) error {

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
		profile, err := s.Profile(pid)
		if err != nil {
			applog.Errorf("Unable to read profile %s from store: %s", pid, err.Error())
			continue
		}
		profiles = append(profiles, profile)
	}

	return profiles, nil
}

func (s *RedisStore) TimelineRange(pid string, status string, ts time.Time, before int, after int) ([]*FormattedItem, error) {

	score := itemScore(ts)

	var key string
	if status == "p" {
		key = possiblyKey(pid, "ts")
	} else {
		key = maybeKey(pid, "ts")
	}

	vals := make([]string, 0)
	itemkeys := make([]string, 0)

	if after > 0 {
		// log.Print("ZRANGEBYSCORE", " ", key, " ", score, " ", "+Inf", " ", "WITHSCORES", " ", "LIMIT", " ", 0, " ", after+1)
		rs := s.tdb.Command("ZRANGEBYSCORE", key, score, "+Inf", "WITHSCORES", "LIMIT", 0, after)
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
	rs := s.tdb.Command("ZREVRANGEBYSCORE", key, formattedScore, "-Inf", "WITHSCORES", "LIMIT", 0, before+1)
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	vals = rs.ValuesAsStrings()
	for i := 0; i < len(vals); i += 2 {
		itemkeys = append(itemkeys, vals[i])
		itemkeys = append(itemkeys, vals[i+1])
	}

	sourcesKey := sourcesKey(pid)
	items := make([]*FormattedItem, 0)

	for ik := 0; ik < len(itemkeys); ik += 2 {

		key := itemkeys[ik]
		score := itemkeys[ik+1]

		if key[0] == EVENTED_ITEM_PREFIX {
			key = key[1:]
		}

		rs = s.idb.Command("GET", key)
		if !rs.IsOK() {
			applog.Errorf("Could not get key %s from db: %s", key, rs.Error().Error())
			continue
		}

		item_infos := rs.ValuesAsStrings()
		for _, item_info := range item_infos {
			item := &FormattedItem{}

			_ = json.Unmarshal([]byte(item_info), item)

			f, err := strconv.ParseFloat(score, 64)
			if err != nil {
				applog.Errorf("Could not parse score from db as float: %s", err.Error())
				continue

			}

			item.Ts = int64(f)

			item.Added = item.Added / 1000000000
			item.Event = item.Event / 1000000000

			rs = s.tdb.Command("HGET", sourcesKey, key)
			if !rs.IsOK() {
				// Ignore
			} else {
				item.Source = rs.ValueAsString()
			}

			items = append(items, item)
		}
	}
	return items, nil
}

func (s *RedisStore) Item(id string) (*Item, error) {
	itemKey := itemKey(id)

	rs := s.idb.Command("GET", itemKey)
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	item := &Item{}
	_ = json.Unmarshal([]byte(rs.Value()), item)

	return item, nil
}

func (s *RedisStore) AddItem(pid string, ets time.Time, text string, link string, image string, itemid string) (string, error) {

	if itemid == "" {
		rs := s.idb.Command("INCR", itemSeqKey(pid))
		if !rs.IsOK() {
			return "", rs.Error()
		}
		seq, _ := rs.ValueAsInt()

		itemid = itemId(pid, seq)
	}

	itemKey := itemKey(itemid)
	eventedItemKey := eventedItemKey(itemid)

	if exists, _ := s.ItemExists(itemid); exists {
		return itemKey, nil
	}
	tsnano := time.Now().UnixNano()

	etsnano := ets.UnixNano()

	if etsnano > 0 {
		// Fake the precision for event time
		nanos := tsnano - 1000000000*(tsnano/1000000000)
		etsnano = ets.Unix()*1000000000 + nanos
	}

	item := &Item{

		Id:    itemid,
		Text:  text,
		Link:  link,
		Pid:   pid,
		Added: tsnano,
		Event: etsnano,
		Image: image,
	}

	json, err := json.Marshal(item)
	if err != nil {
		return itemKey, err
	}

	rs := s.idb.Command("SET", itemKey, json)
	if !rs.IsOK() {
		return "", rs.Error()
	}

	rs = s.tdb.Command("ZADD", maybeKey(pid, ORDERING_TS), item.Added, itemKey)
	if !rs.IsOK() {
		return itemKey, rs.Error()
	}

	if item.IsEvent() {
		rs = s.tdb.Command("ZADD", maybeKey(pid, ORDERING_TS), item.Event, eventedItemKey)
		if !rs.IsOK() {
			return itemKey, rs.Error()
		}
	}

	s.AddItemToFollowerTimelines(pid, item.Added, item)

	if item.Link != "" && item.Image == "" {
		rs := s.pdb.Command("SADD", ITEMS_NEEDING_IMAGES, itemid)
		if !rs.IsOK() {
			return itemKey, rs.Error()
		}
	}

	return itemKey, nil
}

// lifetime is in seconds
func (s *RedisStore) AddTemporaryItem(item *Item, lifetime int) (string, error) {
	itemKey, err := s.AddItem(item.Pid, time.Unix(0, item.Event), item.Text, item.Link, item.Image, item.Id)

	if err == nil {
		rs := s.pdb.Command("EXPIRE", itemKey, lifetime)
		if !rs.IsOK() {
			applog.Errorf("Could not set expiry for temporary item %s: %s", itemKey, rs.Error().Error())
		}

	}

	return itemKey, err

}

func (s *RedisStore) ItemExists(id string) (bool, error) {
	rs := s.idb.Command("EXISTS", itemKey(id))
	if !rs.IsOK() {
		return false, rs.Error()
	}

	val, err := rs.ValueAsBool()
	return val, err
}

func (s *RedisStore) UpdateItem(item *Item) error {
	itemKey := itemKey(item.Id)

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

func (s *RedisStore) DeleteMaybeItems(pid string) error {
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
func (s *RedisStore) Follow(pid string, followpid string) error {
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
func (s *RedisStore) Unfollow(pid string, followpid string) error {

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

func (s *RedisStore) Promote(pid string, id string) error {

	itemKey := itemKey(id)

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

	tsnano := time.Now().UnixNano()
	maybe_key := maybeKey(pid, ORDERING_TS)

	rs = s.tdb.Command("ZADD", maybe_key, tsnano, itemKey)
	if !rs.IsOK() {
		return rs.Error()
	}

	item, err := s.Item(id)
	if err != nil {
		return err
	}

	if item.Event > 0 {
		eventedItemKey := eventedItemKey(id)
		rs = s.tdb.Command("ZADD", maybe_key, item.Event, eventedItemKey)
		if !rs.IsOK() {
			return rs.Error()
		}
	}

	s.AddItemToFollowerTimelines(pid, tsnano, item)

	return nil

}

func (s *RedisStore) Demote(pid string, id string) error {

	// TODO: abort if item is not in possibly timeline
	itemKey := itemKey(id)
	eventedItemKey := eventedItemKey(id)

	maybe_key := maybeKey(pid, ORDERING_TS)

	rs := s.tdb.Command("ZREM", maybe_key, itemKey)
	if !rs.IsOK() {
		return rs.Error()
	}

	rs = s.tdb.Command("ZREM", maybe_key, eventedItemKey)
	if !rs.IsOK() {
		return rs.Error()
	}

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

func (s *RedisStore) Followers(pid string, count int, start int) ([]*Profile, error) {
	rs := s.pdb.Command("ZRANGE", followersKey(pid), start, start+count)
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	profiles := make([]*Profile, 0)

	vals := rs.ValuesAsStrings()

	for _, pid := range vals {
		profile, err := s.Profile(pid)
		if err != nil {
			// TODO: log
		} else {

			profiles = append(profiles, profile)
		}
	}

	return profiles, nil

}

func (s *RedisStore) Following(pid string, count int, start int) ([]*Profile, error) {
	rs := s.pdb.Command("ZRANGE", followingKey(pid), start, start+count)
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	profiles := make([]*Profile, 0)

	vals := rs.ValuesAsStrings()

	for _, pid := range vals {
		profile, err := s.Profile(pid)
		if err != nil {
			// TODO: log
		} else {
			profiles = append(profiles, profile)
		}

	}

	return profiles, nil

}

func (s *RedisStore) VerifyPassword(pid string, password string) (bool, error) {
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

func (s *RedisStore) SessionId(pid string) (int64, error) {
	sessionId := rand.Int63()
	rs := s.sdb.Command("SET", sessionKey(sessionId), pid)
	if !rs.IsOK() {
		return 0, rs.Error()
	}

	return sessionId, nil
}

func (s *RedisStore) ValidSession(pid string, sessionId int64) (bool, error) {
	rs := s.sdb.Command("GET", sessionKey(sessionId))
	if !rs.IsOK() {
		return false, rs.Error()
	}

	return (rs.ValueAsString() == pid), nil

}

func (s *RedisStore) Feeds(pid string) ([]*Profile, error) {
	rs := s.pdb.Command("SMEMBERS", feedsKey(pid))
	if !rs.IsOK() {
		return nil, rs.Error()
	}

	feeds := make([]*Profile, 0)

	vals := rs.ValuesAsStrings()

	for _, fid := range vals {
		feed, err := s.Profile(fid)
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

			itemid := rs.ValueAsString()

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

	// Ful wildcard is an exception
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

func (s *RedisStore) AddItemToFollowerTimelines(pid string, ts int64, item *Item) error {

	rs := s.pdb.Command("ZRANGE", followersKey(pid), 0, MaxInt)
	if !rs.IsOK() {
		applog.Errorf("Could not list followers for pid %s: %s", pid, rs.Error().Error())
		return rs.Error()
	}

	for _, followerpid := range rs.ValuesAsStrings() {
		s.AddItemToTimeline(followerpid, pid, ts, item.Key())
		if item.IsEvent() {
			s.AddItemToTimeline(followerpid, pid, item.Event, item.EventKey())
		}
	}

	return nil
}

func (s *RedisStore) AddItemToTimeline(pid string, source string, ts int64, itemKey string) error {
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

func (s *RedisStore) RemoveItemFromFollowerTimelines(pid string, itemKey string) error {

	rs := s.pdb.Command("ZRANGE", followersKey(pid), 0, MaxInt)
	if !rs.IsOK() {
		applog.Errorf("Could not list followers for pid %s: %s", pid, rs.Error().Error())
		return rs.Error()
	}

	for _, followerpid := range rs.ValuesAsStrings() {
		s.RemoveItemFromTimeline(followerpid, pid, itemKey)
		s.RemoveItemFromTimeline(followerpid, pid, eventedItemKeyFromItemKey(itemKey))
		// if item.IsEvent() {
		// 	s.RemoveItemFromTimeline(followerpid, pid, item.Event, item.EventKey())
		// }
	}

	return nil
}

func (s *RedisStore) RemoveItemFromTimeline(pid string, source string, itemKey string) error {
	applog.Debugf("Removing item %s from timeline for %s", itemKey, pid)
	sourcesKey := sourcesKey(pid)

	// Only remove if source is same as the source recorded for this timeline
	rs := s.tdb.Command("HGET", sourcesKey, itemKey)
	if !rs.IsOK() {
		applog.Errorf("Could not get a source for item %s for pid %s: %s", itemKey, pid, rs.Error().Error())
		return rs.Error()
	}

	if source != rs.ValueAsString() {
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
