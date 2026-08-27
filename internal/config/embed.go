package config

import _ "embed"

//go:embed default_rules.json
var DefaultRulesJSON []byte

//go:embed default_profile.json
var DefaultProfileJSON []byte
