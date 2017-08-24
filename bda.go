package bda

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

//this package is wrapper for using big data on s3
//and aggregate it using aws-athena
const (
	ProcessUploadSuccess   = 1
	ProcessUploadFail      = 2
	ProcessDownloadSuccess = 3
	ProcessDownloadFail    = 4
	ProcessQuerySuccess    = 5
	ProcessQueryFail       = 6
)

//DbConfig this is struct for DB in Athena
type DbConfig struct {
	Name       string
	SourceBulk string
	ResultBulk string
}

//BDA is a struct for big data analysis
type BDA struct {
	session    *session.Session
	svcS3      *s3.S3
	svcAthena  *athena.Athena
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	awsConfig  *AwsConfig
}

//AwsConfig is a struct for aws user credential configuration
type AwsConfig struct {
	AwsID     string
	AwsKey    string
	AwsRegion string
	DbConfig  map[string]DbConfig
}

//QueryExecID this is query for
type QueryExecID *string

//NewBDA is a function
func NewBDA() *BDA {
	return &BDA{}
}

//NewSession this function will create session for accessing aws services
func NewSession(awsID, awsKey, awsRegion string) (*session.Session, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewStaticCredentials(awsID, awsKey, ""),
	})
	if err != nil {
		return nil, err
	}
	return sess, nil
}

//Init will initialize all services that is needed for big data analysis
func (bda *BDA) Init(conf AwsConfig) {
	bda.session, _ = NewSession(conf.AwsID, conf.AwsKey, conf.AwsRegion)
	bda.svcS3 = s3.New(bda.session)
	bda.svcAthena = athena.New(bda.session, aws.NewConfig().WithRegion(conf.AwsRegion))
	bda.uploader = s3manager.NewUploader(bda.session)
	bda.downloader = s3manager.NewDownloader(bda.session)
	bda.awsConfig = &conf
}

//OpenS3Object this is used to open file in the S3, for streaming purposes
func (bda *BDA) OpenS3Object(bucket, key string) (int, error) {
	if bda.session == nil {
		return ProcessDownloadFail, errors.New("error : session not available")
	}
	if bda.svcS3 == nil {
		return ProcessDownloadFail, errors.New("error : service S3 not available")
	}
	//fmt.Printf("OpenS3Object : %s")
	result, err := bda.svcS3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return ProcessDownloadFail, err
	}
	fmt.Printf("result : %s\n", result.GoString())
	return ProcessDownloadSuccess, nil
}

//ReadS3ObjectToFile this func will read from bucket s3 and save in on a file
func (bda *BDA) ReadS3ObjectToFile(bucket, filename, destination string) (int, error) {
	result, err := bda.svcS3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
	})
	if err != nil {
		return ProcessDownloadFail, errors.New("Failed to get object")
	}
	if len(destination) == 0 {
		destination = filename
	}
	file, err := os.Create(destination)
	if err != nil {
		return ProcessDownloadFail, errors.New("Failed to create file")
	}
	defer file.Close()
	defer result.Body.Close()
	if _, err := io.Copy(file, result.Body); err != nil {
		return ProcessDownloadFail, errors.New("Failed to copy object to file")
	}
	return ProcessDownloadSuccess, nil

}

//SaveS3ObjectFromFile this function will upload a file from local to s3
func (bda *BDA) SaveS3ObjectFromFile(bucket, filename string) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return ProcessUploadFail, err
	}
	defer file.Close()
	_, err = bda.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
		Body:   file,
	})

	if err != nil {
		return ProcessUploadFail, err
	}
	//fmt.Printf("Successfully uploaded %q to %q\n", filename, bucket)
	return ProcessUploadSuccess, nil
}

//SaveS3ObjectFromString this function will save object string to s3
func (bda *BDA) SaveS3ObjectFromString(bucket, filename, body string) (int, error) {
	_, err := bda.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
		Body:   strings.NewReader(body),
	})

	if err != nil {
		return ProcessUploadFail, err
	}
	return ProcessUploadSuccess, nil

}

//PrepareQuery this function will insert query to be prepared on athena, query that is allowed is limited,
//please refer to athena documentation
func (bda *BDA) PrepareQuery(dbName, query string) (*string, error) {
	if len(bda.awsConfig.DbConfig) == 0 {
		return nil, errors.New("db config is not available")
	}
	queryExecutionContext := &athena.QueryExecutionContext{
		Database: aws.String(bda.awsConfig.DbConfig[dbName].Name),
	}
	resultConfiguration := &athena.ResultConfiguration{
		OutputLocation: aws.String(bda.awsConfig.DbConfig[dbName].ResultBulk),
	}
	startQueryExectionRequest := &athena.StartQueryExecutionInput{
		QueryString:           aws.String(query),
		QueryExecutionContext: queryExecutionContext,
		ResultConfiguration:   resultConfiguration,
	}
	startQueryExecutionResult, err := bda.svcAthena.StartQueryExecution(startQueryExectionRequest)
	if err != nil {
		return nil, err
	}
	return startQueryExecutionResult.QueryExecutionId, nil
}

//ExecuteQuery this function will execute the query and return the results
func (bda *BDA) ExecuteQuery(qeID QueryExecID, args ...interface{}) ([]map[string]interface{}, error) { //fmt.Println(args...)
	err := WaitForQueryToComplete(bda.svcAthena, qeID)
	if err != nil {
		return nil, err
	}
	result, err := processResultRows(bda.svcAthena, qeID, args...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

//WaitForQueryToComplete this is for waiting query until finish search from data
func WaitForQueryToComplete(svcAth *athena.Athena, queryExecutionID QueryExecID) error {
	getQueryExecutionRequest := &athena.GetQueryExecutionInput{
		QueryExecutionId: queryExecutionID,
	}
	isQueryStillRunning := true
	for isQueryStillRunning {
		getQueryExecutionResult, err := svcAth.GetQueryExecution(getQueryExecutionRequest)
		if err != nil {
			return err
		}
		//fmt.Printf("waitForQueryComplete : %+v\n", getQueryExecutionResult)
		queryState := getQueryExecutionResult.QueryExecution.Status.State
		switch *queryState {
		case athena.QueryExecutionStateCancelled:
			return errors.New("query execution is cancelled")
		case athena.QueryExecutionStateFailed:
			return errors.New("query execution is failed")
		case athena.QueryExecutionStateSucceeded:
			return nil
		}
	}
	return nil
}

func processResultRows(svcAthn *athena.Athena, qeID QueryExecID, args ...interface{}) ([]map[string]interface{}, error) {
	getQueryResultsRequest := &athena.GetQueryResultsInput{
		QueryExecutionId: qeID,
	}
	getQueryResultsResult, err := svcAthn.GetQueryResults(getQueryResultsRequest)
	if err != nil {
		return nil, err
	}
	columnInfoList := getQueryResultsResult.ResultSet.ResultSetMetadata.ColumnInfo
	if len(columnInfoList) == 0 {
		return nil, errors.New("zero column info")
	}
	var queryResults []map[string]interface{}
	for true {
		rows := getQueryResultsResult.ResultSet.Rows
		for idx, row := range rows {
			var queryResult map[string]interface{}
			if idx != 0 {
				queryResult, err = processRow(row, columnInfoList, args...)
				if err != nil {
					return nil, err
				}
				queryResults = append(queryResults, queryResult)
			}

		}
		if getQueryResultsResult.NextToken == nil {
			return queryResults, nil
		}
		getQueryResultsResult, err = svcAthn.GetQueryResults(getQueryResultsRequest.SetNextToken(*getQueryResultsResult.NextToken))
		if err != nil {
			return queryResults, err
		}
	}
	return queryResults, nil
}

func processRow(row *athena.Row, columnInfoList []*athena.ColumnInfo, columnNames ...interface{}) (map[string]interface{}, error) {
	returnResult := make(map[string]interface{})
	for idx, colInfo := range columnInfoList {
		if row.Data[idx].VarCharValue == nil {
			//fmt.Println("masuk sini nih buat nil")
			returnResult[*colInfo.Name] = nil
		} else {
			returnResult[*colInfo.Name] = formatChange(*colInfo.Type, *row.Data[idx].VarCharValue)

		}
	}
	return returnResult, nil
}

func formatChange(varType string, varValue string) interface{} {
	if &varValue == nil {
		return nil
	}
	//fmt.Println(varValue)
	switch varType {
	case "varchar":
		return varValue
	case "tinyint":
		intData, _ := strconv.ParseInt(varValue, 10, 8)
		return int8(intData)
	case "smallint":
		intData, _ := strconv.ParseInt(varValue, 10, 16)
		return int16(intData)
	case "integer":
		intData, _ := strconv.ParseInt(varValue, 10, 32)
		return int32(intData)
	case "bigint":
		intData, _ := strconv.ParseInt(varValue, 10, 64)
		return int64(intData)
	case "double":
		floatData, _ := strconv.ParseFloat(varValue, 64)
		return floatData
	case "boolean":
		boolData, _ := strconv.ParseBool(varValue)
		return boolData
	case "date":
		return varValue
	case "timestamp":
		return varValue
	default:
		return varValue
	}
}
