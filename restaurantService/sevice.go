package main

import (
	"encoding/json"
	"fmt"
	"sync"

	w "kafkarestaurant/restaurantService/workers"

	"github.com/IBM/sarama"
	"github.com/defskela/logger"
)

type Order struct {
	OrderID int      `json:"order_id"`
	Dishes  []string `json:"dishes"`
}

func initWorkers(n int) chan *w.Worker {
	workersChan := make(chan *w.Worker, n)
	for range n {
		workersChan <- &w.Worker{}
	}
	return workersChan
}

func main() {
	workersChan := initWorkers(3)

	wg := &sync.WaitGroup{}
	config := sarama.NewConfig()

	config.Consumer.Return.Errors = true

	brokers := []string{"localhost:9092"}
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		logger.Error(err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions("order")
	if err != nil {
		logger.Error(err)
		return
	}

	logger.Info("Ожидание сообщений из топика order...")
	for _, partition := range partitions {
		partitionConsumer, err := consumer.ConsumePartition("order", partition, sarama.OffsetNewest)
		if err != nil {
			logger.Warn("Ошибка при создании consumer для партиции", partition, ":", err)
			continue
		}
		defer partitionConsumer.Close()

		wg.Add(1)
		go func(consumer sarama.PartitionConsumer, partition int32) {
			defer wg.Done()
			for message := range consumer.Messages() {
				var order Order
				if err := json.Unmarshal(message.Value, &order); err != nil {
					logger.Warn("Ошибка десериализации сообщения:", err)
					continue
				}
				logger.Debug(fmt.Sprintf("Получено сообщение из партиции %d: %+v", partition, order))
				worker := <-workersChan
				go worker.Work(order.Dishes, workersChan)
			}
		}(partitionConsumer, partition)
	}

	wg.Wait()
}
