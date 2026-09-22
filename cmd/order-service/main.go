// Команда order-service: создание заказов, статусы и оркестрация саги.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/hshsb/shop/internal/order"
)

func main() {
	if err := order.Run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "order-service остановлен с ошибкой: %v\n", err)
		os.Exit(1)
	}
}
