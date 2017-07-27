package storage

import "github.com/influxdata/influxdb/monitor/diagnostics"

const (
	// DefaultBindAddress is the default address to bind to.
	DefaultBindAddress  = ":8082"
	DefaultBindAddress2 = ":8083"
	DefaultBindAddress3 = ":8084"
)

// Config represents a configuration for a HTTP service.
type Config struct {
	Enabled      bool   `toml:"enabled"`
	BindAddress  string `toml:"bind-address"`
	BindAddress2 string `toml:"bind-address2"`
	BindAddress3 string `toml:"bind-address3"`
}

// NewConfig returns a new Config with default settings.
func NewConfig() Config {
	return Config{
		Enabled:      true,
		BindAddress:  DefaultBindAddress,
		BindAddress2: DefaultBindAddress2,
		BindAddress3: DefaultBindAddress3,
	}
}

// Diagnostics returns a diagnostics representation of a subset of the Config.
func (c Config) Diagnostics() (*diagnostics.Diagnostics, error) {
	if !c.Enabled {
		return diagnostics.RowFromMap(map[string]interface{}{
			"enabled": false,
		}), nil
	}

	return diagnostics.RowFromMap(map[string]interface{}{
		"enabled":      true,
		"bind-address": c.BindAddress,
	}), nil
}
