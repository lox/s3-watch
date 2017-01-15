package main

import (
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/davecgh/go-spew/spew"
)

type Notification struct {
	AwsRegion         string `json:"awsRegion"`
	EventName         string `json:"eventName"`
	EventSource       string `json:"eventSource"`
	EventTime         string `json:"eventTime"`
	EventVersion      string `json:"eventVersion"`
	RequestParameters struct {
		SourceIPAddress string `json:"sourceIPAddress"`
	} `json:"requestParameters"`
	S3 struct {
		Bucket struct {
			Arn           string `json:"arn"`
			Name          string `json:"name"`
			OwnerIdentity struct {
				PrincipalID string `json:"principalId"`
			} `json:"ownerIdentity"`
		} `json:"bucket"`
		ConfigurationID string `json:"configurationId"`
		Object          struct {
			ETag      string `json:"eTag"`
			Key       string `json:"key"`
			Sequencer string `json:"sequencer"`
			Size      int    `json:"size"`
		} `json:"object"`
		S3SchemaVersion string `json:"s3SchemaVersion"`
	} `json:"s3"`
	UserIdentity struct {
		PrincipalID string `json:"principalId"`
	} `json:"userIdentity"`
}

type Envelope struct {
	Type    string    `json:"Type"`
	Subject string    `json:"Subject"`
	Time    time.Time `json:"Time"`
	Message string    `json:"Message"`
}

type Daemon struct {
	Queue   *Queue
	Handler *os.File
	Signals chan os.Signal
}

func (d *Daemon) Start() error {
	ch := make(chan *sqs.Message)
	go func() {
		for m := range ch {
			var env Envelope
			var records struct {
				Records []Notification `json:"Records"`
			}

			// unmarshal outer layer
			if err := json.Unmarshal([]byte(*m.Body), &env); err != nil {
				log.Println(err)
				continue
			}

			// unmarshal inner layer
			if err := json.Unmarshal([]byte(env.Message), &records); err != nil {
				log.Println(err)
				continue
			}

			for _, n := range records.Records {
				spew.Dump(n)

				if d.Handler != nil {
					err := executeHandler(d.Handler, []string{}, d.Signals)
					if err != nil {
						log.Println(err)
					}
				}
			}

		}
	}()

	return d.Queue.Receive(ch)
}

func executeHandler(command *os.File, args []string, sigs chan os.Signal) error {
	cmd := exec.Command(command.Name(), args...)
	cmd.Env = os.Environ()
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	go func() {
		sig := <-sigs
		if cmd.Process != nil {
			cmd.Process.Signal(sig)
		}
	}()

	if err := cmd.Run(); err != nil {
		return err
	}

	return nil
}
