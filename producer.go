package main

import (
	"bufio"
	"fmt"
	"github.com/IBM/sarama"
	"os"
)

func main() {
	// Kafka broker address
	brokerList := []string{"localhost:9092"}

	// Topic to produce to
	topic := "dev2.order-trigger"

	// Configuration for the Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// Create a new Kafka producer
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		fmt.Println("Failed to create producer:", err)
		return
	}
	defer producer.Close()

	// Read the message from a file
	filename := "message.txt"
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Failed to open file:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		message := scanner.Text()

		// Produce the message to the Kafka topic
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(message),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			fmt.Println("Failed to produce message:", err)
		} else {
			fmt.Printf("Produced message: partition=%d, offset=%d\n", partition, offset)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}
}
