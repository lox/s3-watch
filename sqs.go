package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)

const queuePolicy = `
{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Principal":"*",
      "Action":"sqs:SendMessage",
      "Resource":"*",
      "Condition":{
        "ArnEquals":{
          "aws:SourceArn":"%s"
        }
      }
    }
  ]
}
`

type Queue struct {
	Name, URL, ARN string
	sess           *session.Session
	svc            *sqs.SQS
}

func newQueue(queueName string, topicARN string, sess *session.Session) (*Queue, error) {
	svc := sqs.New(sess)

	resp, err := svc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]*string{
			"Policy": aws.String(fmt.Sprintf(queuePolicy, topicARN)),
		},
	})
	if err != nil {
		return nil, err
	}

	// manually transform url into ARN, doesn't seem to be provided?
	arn := *resp.QueueUrl
	arn = strings.Replace(arn, "https://", "arn:aws:", -1)
	arn = strings.Replace(arn, ".amazonaws.com", "", -1)
	arn = strings.Replace(arn, ".", ":", -1)
	arn = strings.Replace(arn, "/", ":", -1)

	// subscribe to the SNS topic provided
	_, err = sns.New(sess).Subscribe(&sns.SubscribeInput{
		Protocol: aws.String("sqs"),
		TopicArn: aws.String(topicARN),
		Endpoint: aws.String(arn),
	})
	if err != nil {
		return nil, err
	}

	return &Queue{
		Name: queueName,
		URL:  *resp.QueueUrl,
		ARN:  arn,
		sess: sess,
		svc:  svc,
	}, nil
}

func (q *Queue) Receive(ch chan *sqs.Message) error {
	for _ = range time.NewTicker(time.Millisecond * 100).C {
		resp, err := q.svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(q.URL),
			MaxNumberOfMessages: aws.Int64(1),
			WaitTimeSeconds:     aws.Int64(0),
			VisibilityTimeout:   aws.Int64(0),
		})
		if err != nil {
			return err
		}
		for _, m := range resp.Messages {
			q.svc.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      aws.String(q.URL),
				ReceiptHandle: m.ReceiptHandle,
			})
			ch <- m
		}
	}
	return nil
}

func (q *Queue) Delete() error {
	_, err := q.svc.DeleteQueue(&sqs.DeleteQueueInput{
		QueueUrl: aws.String(q.URL),
	})
	return err
}
