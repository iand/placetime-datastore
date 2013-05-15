package datastore

const (
	DB_PROFILE  = "profile"
	DB_ITEM     = "item"
	DB_TIMELINE = "timeline"
	DB_SESSION  = "session"
)

type Config struct {
	Profile  RedisConfig
	Timeline RedisConfig
	Item     RedisConfig
	Session  RedisConfig
}

type RedisConfig struct {
	Address  string `toml:"address"`
	Database int    `toml:"database"`
	Auth     string `toml:"auth"`
	PoolSize int    `toml:"poolsize"`
}

var DefaultConfig Config = Config{
	Profile: RedisConfig{
		Database: 0,
		Address:  "localhost:6379",
		PoolSize: 20,
	},
	Timeline: RedisConfig{
		Database: 1,
		Address:  "localhost:6379",
		PoolSize: 20,
	},
	Item: RedisConfig{
		Database: 2,
		Address:  "localhost:6379",
		PoolSize: 20,
	},
	Session: RedisConfig{
		Database: 3,
		Address:  "localhost:6379",
		PoolSize: 20,
	},
}
