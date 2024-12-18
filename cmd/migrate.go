package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"regexp"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/qdrant/go-client/qdrant"
)

func parseConnectionString(connStr string) (connectionType string, host string, port int, collection string, tls bool, apiKey string, err error) {
	r, err := regexp.Compile(`^(?P<connectionType>\w+):///(?P<protocol>(http|https))://(?P<host>[\w-.]+):(?P<port>\d+)/(?P<collection>[\w.-]+)(\?apiKey=(?P<apiKey>.+))?$`)
	if err != nil {
		return "", "", 0, "", false, "", fmt.Errorf("failed to compile regexp: %w", err)
	}

	m := r.FindStringSubmatch(connStr)
	if m == nil {
		return "", "", 0, "", false, "", fmt.Errorf("failed to parse connection string: %s", connStr)
	}
	var protocol, foundPort string

	for i, name := range r.SubexpNames() {
		switch name {
		case "connectionType":
			connectionType = m[i]
		case "protocol":
			protocol = m[i]
		case "host":
			host = m[i]
		case "port":
			foundPort = m[i]
		case "collection":
			collection = m[i]
		case "apiKey":
			apiKey = m[i]
		}
	}

	port, err = strconv.Atoi(foundPort)

	if err != nil {
		return "", "", 0, "", false, "", fmt.Errorf("failed to parse port: %s: %w", foundPort, err)
	}

	return connectionType, host, port, collection, protocol == "https", apiKey, nil
}

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate data from a data source to Qdrant",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		source, _ := cmd.Flags().GetString("source")
		target, _ := cmd.Flags().GetString("target")

		sourceType, sourceHost, sourcePort, sourceCollection, sourceTLS, sourceAPIKey, err := parseConnectionString(source)
		if err != nil {
			return fmt.Errorf("failed to parse source connection string: %w", err)
		}
		targetType, targetHost, targetPort, targetCollection, targetTLS, targetAPIKey, err := parseConnectionString(target)
		if err != nil {
			return fmt.Errorf("failed to parse target connection string: %w", err)
		}

		if sourceType != "qdrant" {
			return fmt.Errorf("unsupported source type: %s", sourceType)
		}
		if targetType != "qdrant" {
			return fmt.Errorf("unsupported target type: %s", targetType)
		}

		tlsConfig := tls.Config{
			InsecureSkipVerify: true,
		}

		sourceClient, err := qdrant.NewClient(&qdrant.Config{
			Host:      sourceHost,
			Port:      sourcePort,
			APIKey:    sourceAPIKey,
			UseTLS:    sourceTLS,
			TLSConfig: &tlsConfig,
		})
		if err != nil {
			return fmt.Errorf("failed to create source client: %w", err)
		}

		targetClient, err := qdrant.NewClient(&qdrant.Config{
			Host:      targetHost,
			Port:      targetPort,
			APIKey:    targetAPIKey,
			UseTLS:    targetTLS,
			TLSConfig: &tlsConfig,
		})
		if err != nil {
			return fmt.Errorf("failed to create target client: %w", err)
		}

		fmt.Printf("Migrating data from %s %s:%d/%s to %s %s:%d/%s\n", sourceType, sourceHost, sourcePort, sourceCollection, targetType, targetHost, targetPort, targetCollection)

		sourcePointCount, err := sourceClient.Count(ctx, &qdrant.CountPoints{
			CollectionName: sourceCollection,
		})
		if err != nil {
			return fmt.Errorf("failed to count points in source: %w", err)
		}

		fmt.Printf("Source collection has approximately %d points\n", sourcePointCount)

		limit, _ := cmd.Flags().GetUint32("batch-size")
		var offset *qdrant.PointId

		for {
			fmt.Printf("Scrolling data from source starting with %s\n", offset)

			resp, err := sourceClient.GetPointsClient().Scroll(ctx, &qdrant.ScrollPoints{
				CollectionName: sourceCollection,
				Offset:         offset,
				Limit:          &limit,
				WithPayload:    qdrant.NewWithPayload(true),
				WithVectors:    qdrant.NewWithVectors(true),
			})
			if err != nil {
				return fmt.Errorf("failed to scroll date from source: %w", err)
			}

			points := resp.GetResult()
			offset = resp.GetNextPageOffset()

			var targetPoints []*qdrant.PointStruct

			for _, point := range points {
				targetPoints = append(targetPoints, &qdrant.PointStruct{
					Id:      point.Id,
					Payload: point.Payload,
					Vectors: point.Vectors,
				})
			}

			_, err = targetClient.Upsert(ctx, &qdrant.UpsertPoints{
				CollectionName: targetCollection,
				Points:         targetPoints,
			})

			if err != nil {
				return fmt.Errorf("failed to insert data into target: %w", err)
			}

			if offset == nil {
				break
			}
		}

		fmt.Println("Finished migration")

		targetPointCount, err := targetClient.Count(ctx, &qdrant.CountPoints{
			CollectionName: targetCollection,
		})
		if err != nil {
			return fmt.Errorf("failed to count points in target: %w", err)
		}

		fmt.Printf("Target collection has approximately %d points\n", targetPointCount)

		return nil
	},
}

func init() {
	migrateCmd.Flags().StringP("source", "s", "", "Data source")
	err := migrateCmd.MarkFlagRequired("source")
	if err != nil {
		panic(err)
	}
	migrateCmd.Flags().StringP("target", "t", "", "Data target")
	err = migrateCmd.MarkFlagRequired("target")
	if err != nil {
		panic(err)
	}
	migrateCmd.Flags().Uint32P("batch-size", "b", 100, "Batch size")
}
