// Before running this script, run `export AWS_SHARED_CREDENTIALS_FILE="<AWS_CREDENTIALS_FILE_PATH>"` where
// AWS_CREDENTIALS_FILE_PATH represents the file having AWS credentials.
package main

import (
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sns"
	"fmt"
	"os"
	"time"
	"strconv"
	"encoding/json"
	"sync/atomic"
	"sync"
)

const (
	region = "us-west-1"
	queue_name = "benchmark-queue"
	sns_topic_name = "benchmark-topic"
	messageCount = 10000
	enque_parallelism = 10
	deque_parallelism = 10
)
var sqs_client *sqs.SQS
var sns_client *sns.SNS

func main() {
	// Setup SNS, SQS and Create & Subscribe to a Topic
	createSQSClient()
	createSNSClient()
	queue_url := createSQSQueue()
	topicARN := createSNSTopicAndSubscribeSQS(queue_url, sns_topic_name)

	message := getMessage()
	mode := os.Getenv("mode")       // Use "export mode=e" or "export mode=d"
	if mode == "e" {
		fmt.Println("Starting Publishing to SNS Topic")
		BulkPublishToTopic(messageCount, topicARN, message)
	} else if mode == "d" {
		fmt.Println("Starting dequeuer")
		BulkDequeuer(messageCount, queue_url)
	} else {
		fmt.Println("Invalid Flag, Exiting")
		return
	}
}

func BulkPublishToTopic(itemsCount uint64, topicARN string, message string) {
	var count uint64 = 1
	var wg sync.WaitGroup
	wg.Add(enque_parallelism)
	for i:=0; i<enque_parallelism; i++ {
		go func() {
			defer wg.Done()
			for ;count <= itemsCount-enque_parallelism+1; {
				publishToTopic(topicARN, count, message)
				atomic.AddUint64(&count, 1)
			}
		}()
	}
	wg.Wait()
}

func BulkDequeuer(itemsCount uint64, queue_url string) {
	var totalLatency, count uint64
	count = 1
	var wg sync.WaitGroup
	wg.Add(deque_parallelism)
	for i:=0; i<deque_parallelism; i++ {
		go func() {
			defer wg.Done()
			for {
				latency := dequeue(queue_url)
				if count >= itemsCount {
					fmt.Println("Average Latency for last item is ", totalLatency / count)
					break
				}
				if latency != 0 {
					atomic.AddUint64(&count, 1)
					atomic.AddUint64(&totalLatency, latency)
				}
			}
		}()
	}
	wg.Wait()
}

func getMessage() string{
	payload := map[string]interface{}{
	        "src": "972525626731",
	        "dst": "972502224696",
	        "prefix": "972502224696",
	        "url": "",
	        "method": "POST",
	        "text": "\u05dc\u05e7\u05d5\u05d7 \u05d9\u05e7\u05e8 \u05e2\u05e7\u05d1 \u05ea\u05e7\u05dc\u05d4 STOP",
	        "log_sms": "true",
	        "message_uuid": "ffe2bb44-d34f-4359-a7d7-217bf4e9f705",
	        "message_time": "2017-07-13 13:12:47.046303",
	        "carrier_rate": "0.0065",
	        "carrier_amount": "0.013",
	        "is_gsm": false,
	        "is_unicode": true,
	        "units": "2",
	        "auth_info": map[string]interface{}{
		        "auth_id": "MANZE1ODRHYWFIZGMXNJ",
			"auth_token": "NWRjNjU3ZDJhZDM0ZjE5NWE5ZWRmYTNmOGIzNGZm",
			"api_id": "de124d64-6186-11e7-920b-0600a1193e9b",
			"api_method": "POST",
			"api_name": "/api/v1/Message/",
			"account_id": "48844",
			"subaccount_id": "0",
			"parent_auth_id": "MANZE1ODRHYWFIZGMXNJ",
	        },
	}
	payloadBytes, _ := json.Marshal(payload)
	message := string(payloadBytes)
	return message
}

func createSNSClient() {
	// Get AWS credentials from $HOME/.aws/credentials file
	// Or Use "export AWS_SHARED_CREDENTIALS_FILE="<AWS_CREDENTIALS_FILE_PATH>""
	creds := credentials.NewSharedCredentials("", "")

	// Create aws session
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: creds,
	}))

	// Create a SNS service client.
        sns_client = sns.New(sess)
}

func createSQSClient() {
	// Get AWS credentials from $HOME/.aws/credentials file
	// Or Use "export AWS_SHARED_CREDENTIALS_FILE="<AWS_CREDENTIALS_FILE_PATH>""
	creds := credentials.NewSharedCredentials("", "")

	// Create aws session
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: creds,
	}))

	// Create a SQS service client.
        sqs_client = sqs.New(sess)
}

func publishToTopic(topicARN string, index uint64, message string) {
	str_index := fmt.Sprintf("%d", index)
	str_timestamp := fmt.Sprintf("%d", time.Now().UnixNano())
	params := &sns.PublishInput{
		Message: &message,
		MessageAttributes: map[string]*sns.MessageAttributeValue{
			"EnqueueTime": {
				DataType:    aws.String("Number"),
				StringValue: aws.String(str_timestamp),
			},
			"Index": {
				DataType:    aws.String("Number"),
				StringValue: aws.String(str_index),
			},
		},
		TopicArn: &topicARN,
	}
	result, err := sns_client.Publish(params)
	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println("SNS Publish Error:", err, result)
		os.Exit(1)
	}
	//fmt.Println("Published SNS Message: ", result.MessageId)
}

func dequeue(queue_url string) (uint64){
	result, err := sqs_client.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
		    aws.String(sqs.MessageSystemAttributeNameSentTimestamp),    // Get only sent timestamp
		},
		MessageAttributeNames: []*string{
		    aws.String(sqs.QueueAttributeNameAll),      // Get all message attributes
		},
		QueueUrl:            &queue_url,
		MaxNumberOfMessages: aws.Int64(1),      // Return only 1 message
		VisibilityTimeout:   aws.Int64(1),      // Make message invisible for 1 second
		WaitTimeSeconds:     aws.Int64(0),      // Short polling
	})
	if err != nil {
		fmt.Println("Dequeue Error: ", err)
		os.Exit(1)
	}
	if len(result.Messages) == 0 {
		//fmt.Println("Dequeue Received no messages")
		return 0
	}

	// Get latency in nanoseconds between enqueue and dequeue. Since SNS sends the message attributes as part
	// of body, we need to get it from there.
	message := result.Messages[0]
	type MessageBody struct {
		Type              string    `json:"Type"`
		MessageID         string    `json:"MessageId"`
		TopicArn          string    `json:"TopicArn"`
		Message           string    `json:"Message"`
		Timestamp         time.Time `json:"Timestamp"`
		SignatureVersion  string    `json:"SignatureVersion"`
		Signature         string    `json:"Signature"`
		SigningCertURL    string    `json:"SigningCertURL"`
		UnsubscribeURL    string    `json:"UnsubscribeURL"`
		MessageAttributes struct {
			EnqueueTime struct {
				Type  string `json:"Type"`
				Value string `json:"Value"`
			} `json:"EnqueueTime"`
			Index struct {
				Type  string `json:"Type"`
				Value string `json:"Value"`
			} `json:"Index"`
		} `json:"MessageAttributes"`
	}
	var body MessageBody
	json.Unmarshal([]byte(*message.Body), &body)
	enqueue_time := body.MessageAttributes.EnqueueTime.Value
	enqueue_time_nanosec, _ := strconv.ParseUint(enqueue_time, 10, 0)
	latency := uint64(time.Now().UnixNano()) - enqueue_time_nanosec
	//fmt.Println("Latency(ns): ", latency)

	// Delete the message after successful dequeue
	_, err = sqs_client.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &queue_url,
		ReceiptHandle: message.ReceiptHandle,
	})
	if err != nil {
		fmt.Println("Delete Error: ", err)
		os.Exit(1)
	}

	return latency
}

func createSNSTopicAndSubscribeSQS(queueURL string, topicName string) string{
	// Create a SNS Topic. If already exists, it doesn't re-create but simply returns it.
	create_result, err := sns_client.CreateTopic(&sns.CreateTopicInput{Name: &topicName})
	if err != nil {
                fmt.Println("SNS Create Topic Error: ", err)
                os.Exit(1)
        }

	// Get SQS queue ARN
	attributesOutput, err := sqs_client.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{aws.String(sqs.QueueAttributeNameQueueArn)}, QueueUrl: &queueURL})
	if err != nil {
                fmt.Println("SQS GetQueueAttributes Error: ", err)
                os.Exit(1)
        }
	sqsARN := attributesOutput.Attributes[sqs.QueueAttributeNameQueueArn]

	// Subscribe SQS to this Topic
	topicARN := create_result.TopicArn
	protocol := "sqs"
	sns_client.Subscribe(&sns.SubscribeInput{Protocol: &protocol, TopicArn: topicARN, Endpoint: sqsARN})

	// Add permission for SNS to push to SQS by adding a policy.
	// Note that we're allowing ALL to SendMessage, this can be made limited to SNS Topic, but leaving that for simplicity.
	policyString := map[string]interface{}{
		"Version": "2012-10-17",
                "Id": *sqsARN + "/SQSDefaultPolicy",
                "Statement": []map[string]interface{}{
	                {
		                "Sid": "Sid12345",
		                "Effect": "Allow",
		                "Principal": map[string]string{
			                "AWS": "*",
		                },
		                "Action": "SQS:SendMessage",
		                "Resource": *sqsARN,
	                },
                },
	}
	policyBytes, _ := json.Marshal(policyString)
	policy := string(policyBytes)
	result, err := sqs_client.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		Attributes: map[string]*string{sqs.QueueAttributeNamePolicy: &policy}, QueueUrl: &queueURL})
        if err != nil {
                fmt.Println("SQS SetPolicy Error: ", err, result)
                os.Exit(1)
        }

	return *topicARN
}

func createSQSQueue() string {
	// Check if already exists
	get_result, _ := sqs_client.GetQueueUrl(&sqs.GetQueueUrlInput{
	        QueueName: aws.String(queue_name),
	})
	if get_result.QueueUrl != nil {
		fmt.Println("Queue already exists:", *get_result.QueueUrl)
		return *get_result.QueueUrl
	}

	// Create new queue
	create_result, err := sqs_client.CreateQueue(&sqs.CreateQueueInput{
                QueueName: aws.String(queue_name),
	})
        if err != nil {
                fmt.Println("Create Error: ", err)
                os.Exit(1)
        }
        fmt.Println("Successfully created new queue: ", *create_result.QueueUrl)
	return *create_result.QueueUrl
}
