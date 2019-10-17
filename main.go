
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

  rand.Seed(time.Now().UnixNano())

  fmt.Println("upsert")
  for i := 0; i < 2; i++ {

    test:= &TestInfo{
      Name: "First Last",
      First: "First",
      Last: "Last",
      Value: strconv.Itoa(rand.Intn(1000)),
    }

    updated, err := upsert(&TestKey{ Id: fmt.Sprintf("thisisatest%d", i) }, test, svc)
    if err != nil {
      fmt.Println(err)
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

  fmt.Println("items")
  results, err := items(&TestKey{ Id: "thisisatest0"}, svc)
  if err != nil {
    fmt.Println(err)
  }
  for _, i := range results {
   fmt.Printf(
      "Id: %s - Name: %s - First: %s - Last: %s - Value: %s\n",
      i.Id,
      i.Info.Name,
      i.Info.First,
      i.Info.Last,
      i.Info.Value,
    )
  }

  if err = delete(&TestKey{ Id: "thisisatest0"}, svc); err != nil {
    fmt.Println(err)
  }

  fmt.Println("item")
  if item, err := item(&TestKey{ Id: "thisisatest1"}, svc); err != nil {
    fmt.Println(err)
  } else if item == nil {
    fmt.Println("Item is nil")
  } else {
   fmt.Printf(
      "Id: %s - Name: %s - First: %s - Last: %s - Value: %s\n",
      item.Id,
      item.Info.Name,
      item.Info.First,
      item.Info.Last,
      item.Info.Value,
    )
  }
}

func delete(k *TestKey, svc *dynamodb.DynamoDB) error {
  key, err := dynamodbattribute.MarshalMap(*k)
  if err != nil {
    return err
  }

  if out, err := svc.DeleteItem(&dynamodb.DeleteItemInput{
    TableName: aws.String("Test"),
    ReturnValues: aws.String("ALL_OLD"),
    Key: key,
  }); err != nil {
    return err
  } else {
    if len(out.Attributes) == -1 {
      fmt.Println("Nothing deleted")
    }
  }

  return nil

}

func item(k *TestKey, svc *dynamodb.DynamoDB) (*Test, error) {

  key, err := dynamodbattribute.MarshalMap(*k)
  if err != nil {
    return nil, err
  }

  out, err := svc.GetItem(&dynamodb.GetItemInput{
    TableName: aws.String("Test"),
    Key: key,
  })
  if err != nil {
    return nil, err
  }

  if len(out.Item) == 0 {
    return nil, nil
  }

  t := &Test{}
  err = dynamodbattribute.UnmarshalMap(out.Item, &t)
  if err != nil {
    return nil, err
  }

  return t, nil
}

func items(k *TestKey, svc *dynamodb.DynamoDB) ([]*Test, error) {

  key, err := dynamodbattribute.MarshalMap(*k)
  if err != nil {
    return nil, err
  }

  out, err := svc.Query(&dynamodb.QueryInput{
    TableName: aws.String("Test"),
    KeyConditionExpression: aws.String("id=:id"),
    ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
      ":id": key["id"],
    },
  })
  if err != nil {
    return nil, err
  }

  var items []*Test

  for _, item := range out.Items {
    t := &Test{}
    err = dynamodbattribute.UnmarshalMap(item, &t)
    if err != nil {
      return nil, err
    }
    items = append(items, t)
  }

  return items, nil
}

func upsert(k *TestKey, test *TestInfo, svc *dynamodb.DynamoDB) (*Test, error) {

  key, err := dynamodbattribute.MarshalMap(*k)
  if err != nil {
    return nil, err
  }

  update, err := dynamodbattribute.MarshalMap(*test)
  if err != nil {
    return nil, err
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
    return nil, err
  }

  updated := Test{}
  err = dynamodbattribute.UnmarshalMap(result.Attributes, &updated)
  if err != nil {
    return nil, err
  }

  return &updated, nil

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
