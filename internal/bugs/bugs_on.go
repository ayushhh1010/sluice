//go:build sluice_bugs

package bugs

var enabled = map[string]bool{}

// Planted bugs:
//
//	ack-wrong-offset  sequencer replies with base offset + 1 for one batch
//	drop-acked-batch  sequencer acks a batch but leaves it out of the entry
//	ack-before-commit agent acks produces as soon as the segment PUT lands
//	ignore-fence      leader treats a lost log slot as its own success
func On(name string) bool { return enabled[name] }

func Set(name string, on bool) { enabled[name] = on }
