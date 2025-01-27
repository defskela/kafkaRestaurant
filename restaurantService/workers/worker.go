package workers

import (
	"math/rand"
	"time"

	"github.com/defskela/logger"
)

type Worker struct {
	IsWork bool
	Status string
}

func (w *Worker) Work(dishes []string, readyWorkers chan *Worker) {
	w.IsWork = true
	w.Status = "В обработке"
	logger.Info("Заказ принят в работу статус -", w.Status)
	time.Sleep(time.Duration(rand.Intn(3)+2) * time.Second)
	for _, dish := range dishes {
		w.Status = "Готовится блюдо: " + dish
		logger.Info("Готовится блюдо: " + dish)
		time.Sleep(time.Duration(rand.Intn(3)+2) * time.Second)
	}

	w.Status = "Готово"
	logger.Info("Заказ готов")
	w.IsWork = false
	readyWorkers <- w
}
