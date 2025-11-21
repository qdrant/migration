package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strconv"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/pterm/pterm"
	"google.golang.org/grpc"

	"github.com/qdrant/go-client/qdrant"
)

const (
	HTTPS = "https"

	DefaultHTTPPort  = 80
	DefaultHTTPSPort = 443
)

func connectToQdrant(globals *Globals, host string, port int, apiKey string, useTLS bool, maxMessageSize int) (*qdrant.Client, error) {
	debugLogger := logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		pterm.Debug.Printf(msg, fields...)
	})

	var grpcOptions []grpc.DialOption

	if globals.Trace {
		pterm.EnableDebugMessages()
		loggingOptions := logging.WithLogOnEvents(logging.StartCall, logging.FinishCall, logging.PayloadSent, logging.PayloadReceived)
		grpcOptions = append(grpcOptions, grpc.WithChainUnaryInterceptor(logging.UnaryClientInterceptor(debugLogger, loggingOptions)))
		grpcOptions = append(grpcOptions, grpc.WithChainStreamInterceptor(logging.StreamClientInterceptor(debugLogger, loggingOptions)))
	}
	if globals.Debug {
		pterm.EnableDebugMessages()
		loggingOptions := logging.WithLogOnEvents(logging.StartCall, logging.FinishCall)
		grpcOptions = append(grpcOptions, grpc.WithChainUnaryInterceptor(logging.UnaryClientInterceptor(debugLogger, loggingOptions)))
		grpcOptions = append(grpcOptions, grpc.WithChainStreamInterceptor(logging.StreamClientInterceptor(debugLogger, loggingOptions)))
	}

	if maxMessageSize != 0 {
		grpcOptions = append(grpcOptions, grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageSize),
		))
	}

	tlsConfig := tls.Config{
		InsecureSkipVerify: globals.SkipTlsVerification,
	}

	client, err := qdrant.NewClient(&qdrant.Config{
		Host:                   host,
		Port:                   port,
		APIKey:                 apiKey,
		UseTLS:                 useTLS,
		TLSConfig:              &tlsConfig,
		GrpcOptions:            grpcOptions,
		SkipCompatibilityCheck: true,
	})
	if err != nil {
		if port != 6334 {
			pterm.Error.Println("The connection to Qdrant failed. Since you specified a port other than 6334, the likely reason is that you did not connect to Qdrant's GRPC endpoint, which defaults to port 6334.")
		}
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	return client, nil
}

func getPort(u *url.URL) (int, error) {
	if u.Port() != "" {
		sourcePort, err := strconv.Atoi(u.Port())
		if err != nil {
			return 0, fmt.Errorf("failed to parse source port: %w", err)
		}
		return sourcePort, nil
	} else if u.Scheme == HTTPS {
		return DefaultHTTPSPort, nil
	}

	return DefaultHTTPPort, nil
}

func parseQdrantUrl(urlStr string) (host string, port int, tls bool, err error) {
	parsedUrl, err := url.Parse(urlStr)
	if err != nil {
		return "", 0, false, fmt.Errorf("failed to parse URL: %w", err)
	}

	host = parsedUrl.Hostname()
	tls = parsedUrl.Scheme == HTTPS
	port, err = getPort(parsedUrl)
	if err != nil {
		return "", 0, false, fmt.Errorf("failed to parse port: %w", err)
	}

	return host, port, tls, nil
}

func validateBatchSize(batchSize int) error {
	if batchSize < 1 {
		return fmt.Errorf("batch size must be greater than 0, got: %d", batchSize)
	}
	return nil
}

func displayMigrationStart(sourceProvider, sourceCollection, targetCollection string) {
	pterm.DefaultSection.Println("Starting Migration To Qdrant")

	from := fmt.Sprintf("%s@%s", sourceCollection, sourceProvider)
	to := fmt.Sprintf("%s@qdrant", targetCollection)

	table := pterm.TableData{
		{pterm.FgLightCyan.Sprint("From → To:"), pterm.FgLightGreen.Sprintf("%s  →  %s", from, to)},
	}

	_ = pterm.DefaultTable.
		WithHasHeader(false).
		WithBoxed(true).
		WithData(table).
		Render()

	pterm.Println()
}

func displayMigrationProgress(bar *pterm.ProgressbarPrinter, offsetCount uint64) {
	if offsetCount > 0 {
		pterm.Info.Printfln("Starting from offset %d", offsetCount)
		bar.Add(int(offsetCount))
	} else {
		pterm.Info.Printfln("Starting from the beginning")
	}
	fmt.Print("\n")
}

func arbitraryIDToUUID(id string) *qdrant.PointId {
	// If already a valid UUID, use it directly
	if _, err := uuid.Parse(id); err == nil {
		return qdrant.NewIDUUID(id)
	}

	// Otherwise create a deterministic UUID based on the ID
	deterministicUUID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(id))
	return qdrant.NewIDUUID(deterministicUUID.String())
}

// Provides helpful error messages for HTTP responses from ElasticSearch/OpenSearch.
func handleHTTPError(statusCode int, responseBody map[string]any, source string) error {
	if statusCode == 401 {
		return fmt.Errorf("failed to authenticate with %s (status 401): please verify your credentials (username, password, or API key)", source)
	}

	// Check if response contains error details
	if errorInfo, ok := responseBody["error"]; ok {
		return fmt.Errorf("%s returned error (status %d): %v", source, statusCode, errorInfo)
	}

	return fmt.Errorf("%s request failed with status %d", source, statusCode)
}
