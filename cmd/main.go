package main

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/kallydev/chainbase-connector/internal/chainbase"
	"github.com/kallydev/chainbase-connector/internal/config"
	"github.com/kallydev/chainbase-connector/internal/translator"
	"github.com/labstack/echo/v4"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var command = cobra.Command{
	Use: "chainbase-connector",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return viper.BindPFlags(cmd.Flags())
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		// Limit the number of requests per second
		limiter := rate.NewLimiter(rate.Every(time.Second), viper.GetInt(config.FlagThread))

		client, err := chainbase.NewClient(chainbase.WithAPIKey(viper.GetString(config.FlagAPIKey)))
		if err != nil {
			zap.L().Fatal("initialize chainbase client", zap.Error(err))
		}

		server := echo.New()
		server.HideBanner = true
		server.HidePort = true

		server.POST("/", func(c echo.Context) error {
			data, err := io.ReadAll(c.Request().Body)
			if err != nil {
				return fmt.Errorf("read request body: %w", err)
			}

			defer lo.Try(c.Request().Body.Close)

			zap.L().Info("new query request", zap.String("client-ip", c.RealIP()), zap.String("user-agent", c.Request().UserAgent()), zap.ByteString("statement", data))

			// Wait for rate limit
			if err := limiter.Wait(c.Request().Context()); err != nil {
				return fmt.Errorf("wait for rate limit: %w", err)
			}

			response, httpResponse, err := client.DataWarehouse.Query(c.Request().Context(), string(data))
			if err != nil {
				return fmt.Errorf("query chainbase api: %w", err)
			}

			if httpResponse.StatusCode != http.StatusOK {
				return fmt.Errorf("http response has an error: %d %s", httpResponse.StatusCode, httpResponse.Status)
			}

			if response.Code != chainbase.CodeOK {
				return fmt.Errorf("response has an error: %d %s", response.Code, response.Message)
			}

			blob, err := translator.Translate(*response.Data)
			if err != nil {
				return fmt.Errorf("translate result for native format: %w", err)
			}

			return c.Blob(http.StatusOK, "application/octet-stream", blob)
		})

		server.HTTPErrorHandler = func(err error, c echo.Context) {
			zap.L().Error("http error handler", zap.Error(err), zap.String("path", c.Request().URL.Path), zap.String("client-ip", c.RealIP()), zap.String("user-agent", c.Request().UserAgent()))
		}

		return server.Start(viper.GetString(config.FlagListen))
	},
}

func init() {
	command.PersistentFlags().String(config.FlagListen, ":8123", "Listen address of the server")
	command.PersistentFlags().Int(config.FlagThread, 1, "Thread of the number of requests per second")
	command.PersistentFlags().String(config.FlagAPIKey, chainbase.DefaultAPIKey, "API key of Chainbase")
}

func main() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))

	if err := command.Execute(); err != nil {
		zap.L().Fatal("execute command", zap.Error(err))
	}
}
