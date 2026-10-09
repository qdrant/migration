//nolint:unused // Only used in builds that leave out a source.
package cmd

import (
	"fmt"

	"github.com/alecthomas/kong"
)

func registerUnavailableCommand(name, help string) {
	commands = append(commands, func() kong.Option {
		return kong.DynamicCommand(name, help+" Only available in the container image.", "", &unavailableCmd{name: name})
	})
}

type unavailableCmd struct {
	Args []string `arg:"" optional:"" passthrough:"all"`

	name string
}

func (r *unavailableCmd) Run(_ *Globals) error {
	return fmt.Errorf("the %s source is not included in this binary, use the container image registry.cloud.qdrant.io/library/qdrant-migration instead", r.name)
}
