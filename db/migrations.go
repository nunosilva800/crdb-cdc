package db

import (
	"embed"
)

//nolint
//go:embed migrations/*
var Migrations embed.FS
