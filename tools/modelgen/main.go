package main

import (
	"fmt"
	"log"
	"os"

	"gorm.io/driver/postgres"
	"gorm.io/gen"
	"gorm.io/gorm"

	sharedconfig "ticket/shared/config"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		var postgresConfig sharedconfig.Postgres
		if err := sharedconfig.Load(sharedconfig.LocalPostgresPath, &postgresConfig); err != nil {
			log.Fatal(err)
		}
		databaseURL = postgresConfig.Connection.URL()
	}

	db, err := gorm.Open(postgres.Open(databaseURL), &gorm.Config{})
	if err != nil {
		log.Fatalf("open database: %v", err)
	}

	generator := gen.NewGenerator(gen.Config{
		// OutPath is required by gorm/gen, but no query code is emitted because
		// this command only registers database models (it does not ApplyBasic).
		OutPath:           "shared/query",
		ModelPkgPath:      "shared/model/entity",
		FieldWithIndexTag: true,
		FieldWithTypeTag:  true,
	})
	generator.UseDB(db)
	generator.WithJSONTagNameStrategy(func(column string) string { return column })

	// PostgreSQL NUMERIC is generated as string by default. The API performs
	// numeric validation and calculations, so retain its existing float64 type.
	generator.GenerateModel(
		"events",
		gen.FieldType("total_tickets", "int"),
		gen.FieldType("ticket_price", "float64"),
	)
	generator.GenerateModel(
		"users",
		gen.FieldJSONTag("password_hash", "-"),
	)
	generator.Execute()

	fmt.Println("generated database models for events and users")
}
