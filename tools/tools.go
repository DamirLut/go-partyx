//go:build tools

// Package tools pins build-time tool dependencies so `make generate` is
// reproducible. See https://go.dev/wiki/Modules#how-can-i-track-tool-dependencies-for-a-module
package tools

import (
	_ "github.com/edmand46/arpack/cmd/arpack"
)
