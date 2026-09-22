// Команда notification-service: уведомления о смене статуса заказа.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/hshsb/shop/internal/notification"
)

func main() {
	if err := notification.Run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "notification-service остановлен с ошибкой: %v\n", err)
		os.Exit(1)
	}
}
