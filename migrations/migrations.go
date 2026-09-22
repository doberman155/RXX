// Package migrations встраивает SQL-миграции в бинарники сервисов,
// чтобы миграции применялись при старте сервиса без внешних файлов.
package migrations

import "embed"

//go:embed user/*.sql product/*.sql order/*.sql notification/*.sql
var FS embed.FS

// Каталоги миграций внутри встроенной файловой системы.
const (
	DirUser         = "user"
	DirProduct      = "product"
	DirOrder        = "order"
	DirNotification = "notification"
)
