package main

import (
	"github.com/gin-gonic/gin"
	"github.com/sak0/prometheus-to-kafka/controller"
	"flag"

	"github.com/sak0/prometheus-to-kafka/pkg/kafka"
	"os/signal"
	"os"
	"github.com/prometheus/log"
)

var (
	brokers 		string
	group 			string
	topic			string
	kafkaVersion	string
)

func main() {
	flag.StringVar(&brokers, "brokers",
		"10.214.150.29:9092,10.214.150.30:9092,10.214.150.31:9092", "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&topic, "topic", "prometheus-sh-tz",
		"Kafka topics to be producer, as a comma separated list")
	flag.StringVar(&group, "group", "xiaozhupeiqifengkuangluma", "default group for consume.")
	flag.StringVar(&kafkaVersion, "version", "2.1.1", "Kafka cluster version")
	flag.Parse()

	stop := make(chan interface{})
	k, err := kafka.NewProducerController(brokers, topic, kafkaVersion)
	if err != nil {
		panic(err)
	}
	go k.SubscribeErrors(stop)
	defer k.Close()

	go func() {
		quit := make(chan os.Signal)
		signal.Notify(quit, os.Kill, os.Interrupt)
		<-quit
		close(stop)
	}()

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	r.POST("/api/v1/prom/remote/write", controller.WriteToKafka)
	log.Fatal(r.Run("0.0.0.0:38080"))
}