package cmd

import (
	"fmt"

	"github.com/alecthomas/kong"
	"github.com/pterm/pterm"
)

type Globals struct {
	Debug   bool             `help:"Enable debug mode."`
	Trace   bool             `help:"Enable trace mode."`
	Version kong.VersionFlag `name:"version" help:"Print version information and quit"`
}

type CLI struct {
	Globals

	Migrate MigrateCmd `cmd:"" help:"Migrate data to Qdrant from other sources."`
}

func Execute(projectVersion, projectBuild string) {
	version := fmt.Sprintf("Version: %s, Build: %s", projectVersion, projectBuild)
	cli := CLI{}
	ctx := kong.Parse(&cli,
		kong.Name("migration"),
		kong.Description("Migrate data to Qdrant from other sources."),
		kong.Vars{
			"version": version,
		})

	err := ctx.Run(&cli.Globals)

	if err != nil {
		fmt.Print("\n")
		pterm.Error.Println(err)
		ctx.Exit(1)
	}
}
