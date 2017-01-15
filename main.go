package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/alecthomas/kingpin"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/rs/xid"
)

func main() {
	var bucket, topic string

	app := kingpin.New("s3-watch",
		"Watch s3 bucket for events and dispatch to a script")

	app.Flag("bucket", "The s3 bucket to watch").
		Required().
		StringVar(&bucket)

	app.Flag("topic", "A specific sns topic to subscribe to (otherwise inferred)").
		StringVar(&topic)

	app.Action(func(c *kingpin.ParseContext) error {
		sess := session.New()
		guid := xid.New().String()

		if topic == "" {
			var err error
			if topic, err = discoverSNSTopic(bucket, sess); err != nil {
				log.Fatalf("Can't discover SNS topic from bucket: %v", err)
			}
		}

		log.Printf("Creating queue for topic %s", topic)
		queue, err := newQueue(fmt.Sprintf("s3-watch-%s", guid), topic, sess)
		if err != nil {
			log.Fatalf("Can't create SQS queue: %v", err)
		}
		defer cleanup(queue)

		sigs := make(chan os.Signal, 2)
		signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sigs
			log.Printf("Shutting down gracefully...")
			cleanup(queue)
			os.Exit(1)
		}()

		daemon := Daemon{
			Queue:   queue,
			Signals: sigs,
		}

		log.Printf("Watching s3://%s", bucket)
		if err := daemon.Start(); err != nil {
			log.Fatal(err.Error())
		}

		return nil
	})

	kingpin.MustParse(app.Parse(os.Args[1:]))
}

func cleanup(q *Queue) {
	log.Printf("Deleting SQS Queue %s", q.Name)
	if err := q.Delete(); err != nil {
		log.Fatal(err)
	}
}

func discoverSNSTopic(bucket string, sess *session.Session) (string, error) {
	svc := s3.New(sess)

	resp, err := svc.GetBucketNotificationConfiguration(&s3.GetBucketNotificationConfigurationRequest{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return "", err
	}

	if len(resp.TopicConfigurations) == 0 {
		return "", errors.New("No topic notification queues configured")
	}

	return *resp.TopicConfigurations[0].TopicArn, nil
}
