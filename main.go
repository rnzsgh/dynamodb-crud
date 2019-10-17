
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

  fmt.Printf("row count: %d\n", count(svc))

  rand.Seed(time.Now().UnixNano())

  fmt.Println("upsert")
  for i := 0; i < 2; i++ {

    test:= &TestInfo{
      Name: "First Last",
      First: "First",
      Last: "Last",
      Value: strconv.Itoa(rand.Intn(1000)),
    }

    err = upsert(&TestKey{ Id: fmt.Sprintf("thisisatest%d", i) }, test, "Test", ":i", "set i=:i", svc)
    if err != nil {
      fmt.Println(err)
      return
    }
  }

  fmt.Printf("row count: %d\n", count(svc))
  fmt.Println("put")

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

  fmt.Printf("row count: %d\n", count(svc))

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

  fmt.Printf("row count: %d\n", count(svc))
  fmt.Println("delete")
  if err = delete(&TestKey{ Id: "thisisatest0"}, "Test", svc); err != nil {
    fmt.Println(err)
  }

  fmt.Printf("row count: %d\n", count(svc))

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

  fmt.Printf("row count: %d\n", count(svc))
}

func count(svc *dynamodb.DynamoDB) int64 {
  if c, err := rowCount("Test", svc); err != nil {
    fmt.Println(err)
    return 0
  } else {
    return c
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

func rowCount(table string, svc *dynamodb.DynamoDB) (int64, error) {

  out, err := svc.DescribeTable(&dynamodb.DescribeTableInput{ TableName: aws.String(table) })
  if err != nil {
    return 0, err
  }

  return aws.Int64Value(out.Table.ItemCount), nil

}

func upsert(k interface{}, i interface{}, table, entryField, expression string, svc *dynamodb.DynamoDB) error {

  key, err := dynamodbattribute.MarshalMap(k)
  if err != nil {
    return err
  }

  update, err := dynamodbattribute.MarshalMap(i)
  if err != nil {
    return  err
  }
  //fmt.Println(update)

  input := &dynamodb.UpdateItemInput{
    Key:                       key,
    TableName:                 aws.String(table),
    UpdateExpression:          aws.String(expression),
    ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
      entryField: &dynamodb.AttributeValue{M: update},
    },
    ReturnValues:              aws.String("NONE"),
  }

  if _, err = svc.UpdateItem(input); err != nil {
    return err
  }

  return nil
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
