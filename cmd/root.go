package cmd

import (
	"fmt"

	"github.com/alecthomas/kong"
	"github.com/pterm/pterm"
)

type Globals struct {
	Debug               bool             `help:"Enable debug mode."`
	Trace               bool             `help:"Enable trace mode."`
	SkipTlsVerification bool             `help:"Skip TLS verification."`
	Version             kong.VersionFlag `name:"version" help:"Print version information and quit"`
}

type CLI struct {
	Globals
}

var commands []func() kong.Option

func registerCommand[T any](name, help string) {
	commands = append(commands, func() kong.Option {
		return kong.DynamicCommand(name, help, "", new(T))
	})
}

func commandOptions() []kong.Option {
	options := make([]kong.Option, 0, len(commands))
	for _, newCommand := range commands {
		options = append(options, newCommand())
	}
	return options
}

func Execute(projectVersion, projectBuild string) {
	version := fmt.Sprintf("Version: %s, Build: %s", projectVersion, projectBuild)
	cli := CLI{}
	options := []kong.Option{
		kong.Name("migration"),
		kong.Description("Migrate data to Qdrant from other sources."),
		kong.Vars{
			"version": version,
		},
	}
	ctx := kong.Parse(&cli, append(options, commandOptions()...)...)

	err := ctx.Run(&cli.Globals)

	if err != nil {
		fmt.Print("\n")
		pterm.Error.Println(err)
		ctx.Exit(1)
	}
}

func NewParser(args []string) (*kong.Context, error) {
	cli := &CLI{}

	parser, err := kong.New(cli, append([]kong.Option{kong.Bind(&cli.Globals)}, commandOptions()...)...)
	if err != nil {
		return nil, err
	}

	return parser.Parse(args)
}
