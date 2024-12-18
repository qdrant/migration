package main

import (
	"github.com/qdrant/migration/cmd"
)

var (
	projectVersion = "0.0.0"
	projectBuild   = "dev"
)

func main() {
	cmd.Execute(projectVersion, projectBuild)
}
