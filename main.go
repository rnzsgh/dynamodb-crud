
package main

import (
  "fmt"
  "strconv"
  "math/rand"
  "time"

  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/awserr"
  "github.com/aws/aws-sdk-go/aws/session"
  "github.com/aws/aws-sdk-go/service/dynamodb"
  "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type TestKey struct {
  Id string  `json:"id"`
}

type Test struct {
  Id string  `json:"id"`
  Info  TestInfo `json:"i,omitempty"`
}

type TestInfo struct {
  Name string `json:"n"`
  First string `json:"f"`
  Last string `json:"l"`
  Value string `json:"v"`
}

func main() {
  fmt.Println("Start")

  config := &aws.Config{
    Region:   aws.String("us-east-1"),
    Endpoint: aws.String("http://localhost:8000"),
  }

  sess := session.Must(session.NewSession(config))

  svc := dynamodb.New(sess)

  _, err := svc.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String("Test")})

  if err != nil && err.(awserr.Error).Code() == "ResourceNotFoundException" {
    if err = table(svc); err != nil {
      fmt.Println(err)
      return
    }
  } else if err != nil {
    fmt.Println(err)
    return
  }

  k := TestKey{ Id: "thisisatest" }

  key, err := dynamodbattribute.MarshalMap(k)
  if err != nil {
    fmt.Println(err.Error())
    return
  }

  rand.Seed(time.Now().UnixNano())

  test:= TestInfo{
    Name: "First Last",
    First: "First",
    Last: "Last",
    Value: strconv.Itoa(rand.Intn(1000)),
  }

  update, err := dynamodbattribute.MarshalMap(test)
  if err != nil {
    fmt.Println(err.Error())
    return
  }
  //fmt.Println(update)

  input := &dynamodb.UpdateItemInput{
    Key:                       key,
    TableName:                 aws.String("Test"),
    UpdateExpression:          aws.String("set i=:i"),
    ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
      ":i": &dynamodb.AttributeValue{M: update},
    },
    ReturnValues:              aws.String("ALL_NEW"),
  }

  result, err := svc.UpdateItem(input)
  if err != nil {
    fmt.Println(err.Error())
    return
  }

  updated := Test{}
  err = dynamodbattribute.UnmarshalMap(result.Attributes, &updated)
  if err != nil {
    fmt.Println(err.Error())
    return
  }

  fmt.Printf(
    "Id: %s - Name: %s - First: %s - Last: %s - Value: %s\n",
    updated.Id,
    updated.Info.Name,
    updated.Info.First,
    updated.Info.Last,
    updated.Info.Value,
  )
}

func table(svc *dynamodb.DynamoDB) error {

  input := &dynamodb.CreateTableInput{
    AttributeDefinitions: []*dynamodb.AttributeDefinition{
      {
        AttributeName: aws.String("id"),
        AttributeType: aws.String("S"),
      },
    },
    KeySchema: []*dynamodb.KeySchemaElement{
      {
        AttributeName: aws.String("id"),
        KeyType:       aws.String("HASH"),
      },
    },
    ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
      ReadCapacityUnits:  aws.Int64(10),
      WriteCapacityUnits: aws.Int64(10),
    },
    TableName: aws.String("Test"),
  }

  _, err := svc.CreateTable(input)
  if err != nil {
    return err
  }

  fmt.Println("Table created")

  //fmt.Println(result)

  return nil

}
