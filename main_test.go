package main

import (
	"testing"
)

func BenchmarkBulkPublishToTopic(b *testing.B) {
	createSQSClient()
	createSNSClient()
	queue_url := createSQSQueue()
	topicARN := createSNSTopicAndSubscribeSQS(queue_url, sns_topic_name)
	message := getMessage()

	b.ResetTimer()
        for n := 0; n < b.N; n++ {
                BulkPublishToTopic(100, topicARN, message)
        }
}

func BenchmarkBulkDequeuer(b *testing.B) {
	createSQSClient()
	queue_url := createSQSQueue()

	b.ResetTimer()
        for n := 0; n < b.N; n++ {
                BulkDequeuer(100, queue_url)
        }
}