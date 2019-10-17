
package main

import (
  "fmt"
  "strconv"
  "math/rand"
  "errors"
  "time"
  "reflect"

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
  Info *TestInfo `json:"i,omitempty"`
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

  if err = put(
    &Test{
      Id: "thisisatest0",
      Info: &TestInfo{
        Name: "late entry",
        First: "late",
        Last: "entry"},
    },
    "Test",
    svc,
  ); err != nil {
    fmt.Println(err)
    return
  }

  fmt.Println("items")
  results, err := items(&TestKey{ Id: "thisisatest0"}, &Test{}, "Test", svc)
  if err != nil {
    fmt.Println(err)
  }
  for _, i := range results {
    t := i.(*Test)
   fmt.Printf(
      "Id: %s - Name: %s - First: %s - Last: %s - Value: %s\n",
      t.Id,
      t.Info.Name,
      t.Info.First,
      t.Info.Last,
      t.Info.Value,
    )
  }

  if err = delete(&TestKey{ Id: "thisisatest0"}, "Test", svc); err != nil {
    fmt.Println(err)
  }

  fmt.Println("item")
  test := &Test{}
  if err := item(&TestKey{ Id: "thisisatest1"}, test, "Test", svc); err != nil {
    if err.Error() == "NotFound" {
      fmt.Println("Not found")
    } else {
      fmt.Println(err)
    }
  } else {
   fmt.Printf(
      "Id: %s - Name: %s - First: %s - Last: %s - Value: %s\n",
      test.Id,
      test.Info.Name,
      test.Info.First,
      test.Info.Last,
      test.Info.Value,
    )
  }
}

func delete(k interface{}, table string, svc *dynamodb.DynamoDB) error {
  key, err := dynamodbattribute.MarshalMap(k)
  if err != nil {
    return err
  }

  if _, err := svc.DeleteItem(&dynamodb.DeleteItemInput{
    TableName: aws.String(table),
    Key: key,
  }); err != nil {
    return err
  }
  return nil
}

func put(v interface{}, table string, svc *dynamodb.DynamoDB) error {
  i, err := dynamodbattribute.MarshalMap(v)
  if err != nil {
    return err
  }

  if _, err := svc.PutItem(&dynamodb.PutItemInput{
    TableName: aws.String(table),
    Item: i,
  }); err != nil {
    return err
  }
  return nil
}

func item(k interface{}, v interface{}, table string, svc *dynamodb.DynamoDB) error {

  key, err := dynamodbattribute.MarshalMap(k)
  if err != nil {
    return err
  }

  out, err := svc.GetItem(&dynamodb.GetItemInput{
    TableName: aws.String(table),
    Key: key,
  })
  if err != nil {
    return err
  }

  if len(out.Item) == 0 {
    return errors.New("NotFound")
  }

  err = dynamodbattribute.UnmarshalMap(out.Item, v)
  if err != nil {
    return err
  }

  return nil
}

func items(k interface{}, itemType interface{}, table string, svc *dynamodb.DynamoDB) ([]interface{}, error) {

  key, err := dynamodbattribute.MarshalMap(k)
  if err != nil {
    return nil, err
  }

  out, err := svc.Query(&dynamodb.QueryInput{
    TableName: aws.String(table),
    KeyConditionExpression: aws.String("id=:id"),
    ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
      ":id": key["id"],
    },
  })
  if err != nil {
    return nil, err
  }

  var items []interface{}

  for _, item := range out.Items {
    i := reflect.New(reflect.ValueOf(itemType).Elem().Type()).Interface()

    err = dynamodbattribute.UnmarshalMap(item, i)
    if err != nil {
      return nil, err
    }
    items = append(items, i)
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
