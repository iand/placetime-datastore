package datastore

type Profile struct {
	Pid            string `json:"pid"`
	Name           string `json:"name,omitempty"`
	PasswordHash   []byte `json:"-"`
	Bio            string `json:"bio,omitempty"`
	Email          string `json:"email,omitempty"`
	Joined         int64  `json:"joined,omitempty"`
	PossiblyCount  int    `json:"pcount"`
	MaybeCount     int    `json:"mcount"`
	FollowerCount  int    `json:"followercount"`
	FollowingCount int    `json:"followingcount"`
	FeedCount      int    `json:"feedcount"`
	FeedUrl        string `json:"feedurl,omitempty"`
	ParentPid      string `json:"parentpid,omitempty"`
}

type ScoredProfile struct {
	Pid   string  `json:"pid"`
	Score float64 `json:"score"`
}

type FollowingProfile struct {
	Profile
	Reciprocal bool `json:"reciprocal"`
}
