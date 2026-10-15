package commands

// KeySpec says which arguments of a command are keys, using the same scheme
// as Redis's command table. Positions index the full argv, where argv[0] is
// the command name itself:
//
//	First: position of the first key (0 = the command takes no keys)
//	Last:  position of the last key; negative counts from the end, so -1
//	       means "the last argument"
//	Step:  distance between keys (1 = every argument in range is a key)
//
// Examples: GET k -> {1,1,1}; DEL k1 k2 k3 -> {1,-1,1};
// OBJECT ENCODING k -> {2,2,1}.
type KeySpec struct {
	First, Last, Step int
}

var noKeys = KeySpec{}

// keySpecs must have an entry for every registered command. A test enforces
// this, so a new command can't silently skip cluster slot checks.
var keySpecs = map[string]KeySpec{
	"GET":     {1, 1, 1},
	"SET":     {1, 1, 1},
	"EXPIRE":  {1, 1, 1},
	"TTL":     {1, 1, 1},
	"INCR":    {1, 1, 1},
	"APPEND":  {1, 1, 1},
	"PFADD":   {1, 1, 1},
	"PFCOUNT": {1, -1, 1},
	"PFMERGE": {1, -1, 1},
	"DEL":     {1, -1, 1},
	"OBJECT":  {2, 2, 1},
	"PING":    noKeys,
	"INFO":    noKeys,
	"DEBUG":   noKeys,
	"CLUSTER": noKeys,
	"_EXPIRY": noKeys,
}

// KeysOf returns the key arguments of cmd. ok is false if the command has no
// key spec (i.e. it is unknown). Too few arguments just yields fewer keys;
// the handler itself reports the arity error.
func KeysOf(cmd *Command) (keys []string, ok bool) {
	spec, ok := keySpecs[cmd.Name]
	if !ok {
		return nil, false
	}
	if spec.First == 0 {
		return nil, true
	}
	argc := len(cmd.Args) + 1 // + the command name
	last := spec.Last
	if last < 0 {
		last = argc + last
	}
	for pos := spec.First; pos <= last && pos < argc; pos += spec.Step {
		keys = append(keys, cmd.Args[pos-1])
	}
	return keys, true
}
