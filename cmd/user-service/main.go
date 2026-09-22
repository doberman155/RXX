// Команда user-service: регистрация, вход, выпуск JWT и роли USER/ADMIN.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/hshsb/shop/internal/user"
)

func main() {
	if err := user.Run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "user-service остановлен с ошибкой: %v\n", err)
		os.Exit(1)
	}
}
