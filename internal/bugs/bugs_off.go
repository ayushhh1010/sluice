//go:build !sluice_bugs

// Package bugs plants known bugs for testing the checker. In normal builds
// On is a constant false and every hook compiles away; build with
// -tags sluice_bugs to enable them.
package bugs

// On reports whether the named planted bug is enabled.
func On(string) bool { return false }

// Set is a no-op without the sluice_bugs tag.
func Set(string, bool) {}
