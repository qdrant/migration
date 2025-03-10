package cmd

import (
	"fmt"

	"github.com/alecthomas/kong"
	"github.com/pterm/pterm"
)

type Globals struct {
	Debug   bool        `help:"Enable debug mode."`
	Trace   bool        `help:"Enable trace mode."`
	Version VersionFlag `name:"version" help:"Print version information and quit"`
}

type CLI struct {
	Globals

	Migrate MigrateCmd `cmd:"" help:"Migrate data to Qdrant from other sources."`
}

type VersionFlag string

func (v VersionFlag) Decode(_ *kong.DecodeContext) error { return nil }
func (v VersionFlag) IsBool() bool                       { return true }
func (v VersionFlag) BeforeApply(app *kong.Kong, vars kong.Vars) error {
	fmt.Println(vars["version"])
	app.Exit(0)
	return nil
}

func Execute(projectVersion, projectBuild string) {
	version := fmt.Sprintf("Version: %s, Build: %s", projectVersion, projectBuild)
	cli := CLI{
		Globals: Globals{
			Version: VersionFlag(version),
		},
	}
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
