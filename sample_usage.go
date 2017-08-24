package main

import (
	"fmt"

	"github.com/thoecool/bda"
)

//type Athena struct {
//	*client.Client
//}
const (
	AwsID     = "AWS_ID"
	AwsKEY    = "AWS_KEY"
	AwsREGION = "AWS_REGION"
)

var bd *bda.BDA

func main() {
	var dbConfig map[string]bda.DbConfig
	dbConfig = make(map[string]bda.DbConfig)
	dbConfig["cart"] = bda.DbConfig{
		Name:       "cart",
		SourceBulk: "tokopedia/cart/",
		ResultBulk: "s3://aws-athena-query-results-496130839377-ap-southeast-1/",
	}
	awsConf := bda.AwsConfig{
		AwsID:     AwsID,
		AwsKey:    AwsKEY,
		AwsRegion: AwsREGION,
		DbConfig:  dbConfig,
	}

	bd = bda.NewBDA()
	bd.Init(awsConf)

	// this is sampe for inserting data to s3 aws
	_, err := bd.SaveS3ObjectFromString("tokopedia/cart/", "test.txt", "ini percobaan upload")
	if err != nil {
		fmt.Println(err)
	}
	qeID, err := bd.PrepareQuery("cart", "select * from ws_shopping_cart")
	if err != nil {
		fmt.Println(err)
		return
	}
	var x, b string
	results, err := bd.ExecuteQuery(qeID, x, b)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("Result Length : %d\n", len(results))
	for _, result := range results {
		fmt.Printf("cart_id : %d\n", result["cart_id"])
		fmt.Printf("customer_id : %d\n", result["customer_id"])
		fmt.Printf("update_time : %s\n", result["update_time"])
	}
}
