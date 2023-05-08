package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	uuid "github.com/satori/go.uuid"
)

func fail(err error) {
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(-1)
	}
}

type Url struct {
	Url string
}

type SQSSender struct {
	QueueUrl string
	Client   *sqs.SQS
	wg       sync.WaitGroup
}

func (s *SQSSender) sendMessages(messages []map[string]interface{}, id string) {
	var chunk []*sqs.SendMessageBatchRequestEntry
	for i, message := range messages {
		if i%10 == 0 {
			fmt.Printf(".")
		}
		msg, _ := json.Marshal(message)
		chunk = append(chunk, messageEntry(string(msg)))
		if len(chunk) == 10 {
			sendOutput, err := s.Client.SendMessageBatch(&sqs.SendMessageBatchInput{Entries: chunk, QueueUrl: &s.QueueUrl})
			fail(err)

			if len(sendOutput.Failed) > 0 {
				for _, e := range sendOutput.Failed {
					fmt.Printf("Message failed to be sent, reason: %s\n", *(e.Message))
				}
				fail(fmt.Errorf("%d messages failed to be sent", len(sendOutput.Failed)))
			}
			chunk = nil

		}
	}
	if len(chunk) > 0 {

		fmt.Printf(".")
		sendOutput, err := s.Client.SendMessageBatch(&sqs.SendMessageBatchInput{Entries: chunk, QueueUrl: &s.QueueUrl})
		fail(err)

		if len(sendOutput.Failed) > 0 {
			for _, e := range sendOutput.Failed {
				fmt.Printf("Message failed to be sent, reason: %s\n", *(e.Message))
			}
			fail(fmt.Errorf("%d messages failed to be sent", len(sendOutput.Failed)))
		}
	}
	s.wg.Done()

}

func (s *SQSSender) startFetching(u string) {
	resp, err := http.Get(u)
	if err != nil {
		fail(fmt.Errorf("Error while getting data %w", err))
	}
	var message []map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&message)
	if err != nil {
		fail(fmt.Errorf("Error while parsing data %w", err))
	}
	s.wg.Add(1)
	id := u[strings.LastIndex(u, "/")+1:]
	go s.sendMessages(message, id)
	s.wg.Done()

}

func main() {

	s, err := getSqsService()
	fail(err)

	queueName := "test-queue"
	fmt.Printf("Creating SQS queue [%s]\n", queueName)
	output, err := s.CreateQueue(&sqs.CreateQueueInput{QueueName: aws.String(queueName)})
	fail(err)
	fmt.Printf("Queue created, url: %s\n", *output.QueueUrl)

	sender := SQSSender{
		QueueUrl: *output.QueueUrl,
		Client:   s,
	}

	fmt.Printf("Getting data index...\n")
	resp, err := http.Get("https://interview-data-bucket.s3.ap-south-1.amazonaws.com/data-eng-interview-data/index.json")

	if err != nil {
		fail(fmt.Errorf("Error while getting data from s3 %w", err))
	}

	var urls []Url

	err = json.NewDecoder(resp.Body).Decode(&urls)
	if err != nil {
		fail(fmt.Errorf("Error while parsing index %w", err))
	}
	fmt.Printf("Sending messages to queue...")
	for _, url := range urls {
		sender.wg.Add(1)
		go sender.startFetching(url.Url)
	}
	sender.wg.Wait()
}

func messageEntry(body string) *sqs.SendMessageBatchRequestEntry {
	var delay int64 = 1
	id := uuid.NewV4().String()
	return &sqs.SendMessageBatchRequestEntry{
		DelaySeconds: aws.Int64(delay),
		MessageBody:  aws.String(body),
		Id:           aws.String(id),
	}
}

func getSqsService() (*sqs.SQS, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("ap-south-1"),
		Credentials: credentials.NewStaticCredentials("AKID", "SECRET_KEY", "TOKEN"),
		Endpoint:    aws.String("http://localhost:4566"),
	})
	if err != nil {
		return nil, err
	}
	return sqs.New(sess), nil
}
