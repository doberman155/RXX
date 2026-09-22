// Команда product-service: каталог товаров, остатки и резервирование по событиям.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/hshsb/shop/internal/product"
)

func main() {
	if err := product.Run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "product-service остановлен с ошибкой: %v\n", err)
		os.Exit(1)
	}
}
