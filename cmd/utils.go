package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/pterm/pterm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

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

	// Add keepalive parameters to prevent HTTP/2 stream resets
	grpcOptions = append(grpcOptions, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                30 * time.Second, // Send keepalive ping every 30 seconds
		Timeout:             10 * time.Second, // Wait 10 seconds for ping ack before considering the connection dead
		PermitWithoutStream: true,             // Send keepalive pings even without active streams
	}))

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
func handleElasticOpenSearchHTTPError(statusCode int, responseBody map[string]any, source string) error {
	if statusCode == 401 {
		return fmt.Errorf("failed to authenticate with %s (status 401): please verify your credentials (username, password, or API key)", source)
	}

	// Check if response contains error details
	if errorInfo, ok := responseBody["error"]; ok {
		return fmt.Errorf("%s returned error (status %d): %v", source, statusCode, errorInfo)
	}

	return fmt.Errorf("%s request failed with status %d", source, statusCode)
}

const (
	// maxUpsertRetries is the maximum number of retries for upsert operations on transient errors.
	maxUpsertRetries = 5
	// baseRetryDelay is the base delay for exponential backoff.
	baseRetryDelay = 200 * time.Millisecond
)

// isRetryableError checks if an error is retryable (transient errors).
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "Please retry") ||
		strings.Contains(errStr, "too_many_internal_resets") ||
		strings.Contains(errStr, "RST_STREAM") ||
		strings.Contains(errStr, "connection error") ||
		strings.Contains(errStr, "ResourceExhausted")
}

// upsertWithRetry performs an upsert operation with retry logic for transient errors.
// It handles Qdrant's transient consistency errors and HTTP/2 stream resets.
func upsertWithRetry(ctx context.Context, client *qdrant.Client, req *qdrant.UpsertPoints) error {
	var err error
	for attempt := 0; attempt < maxUpsertRetries; attempt++ {
		_, err = client.Upsert(ctx, req)
		if err == nil {
			return nil
		}
		if !isRetryableError(err) {
			break
		}
		// Exponential backoff: 200ms, 400ms, 800ms, 1600ms, 3200ms
		delay := baseRetryDelay * time.Duration(1<<attempt)
		time.Sleep(delay)
	}
	if err != nil {
		return fmt.Errorf("failed to insert data into target: %w", err)
	}
	return nil
}
