package main

import (
	"fmt"

	"github.com/schema-registry/schema"
)

func main() {
	client := schema.NewSchemaRegistryClient("http://capp-schemaregistry.dev-mytaxi.com:8081")
	// schemas, err := client.GetAllSubjects()
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println(len(schemas))

	// schema, _ := client.GetSchemaByID(2665)
	// fmt.Println(schema)

	// types, _ := client.GetSchemaTypes()
	// fmt.Println(types[0])

	// version, _ := client.GetSchemaIDVersion(2665)
	// fmt.Println(version)

	schema, err := client.GetLatestSchemaReferencedBy("com.pickme.events.job.JobCreated")
	fmt.Println(schema)
	fmt.Println(err)
}
