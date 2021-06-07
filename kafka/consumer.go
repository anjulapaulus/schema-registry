package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

type Consumer struct {
	connection sarama.Consumer
}

func CreateConsumerConnection(brokers []string) (Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	conn, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return Consumer{}, err
	}

	return Consumer{connection: conn}, nil
}

func (c *Consumer) Consume(topic string) {
	consumer, err := c.connection.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(consumer.Messages())
}
