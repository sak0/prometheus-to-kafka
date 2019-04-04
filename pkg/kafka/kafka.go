package kafka

import (
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/prometheus/common/model"
	"github.com/Shopify/sarama"
)

type TagValue model.LabelValue

func tagsFromMetric(m model.Metric) map[string]TagValue {
	tags := make(map[string]TagValue, len(m)-1)
	for l, v := range m {
		if l == model.MetricNameLabel {
			continue
		}
		tags[string(l)] = TagValue(v)
	}
	return tags
}

type SamplesRequest struct {
	Metric    TagValue            `json:"metric"`
	Timestamp int64               `json:"timestamp"`
	Value     float64             `json:"value"`
	Tags      map[string]TagValue `json:"tags"`
}

type ProducerController struct {
	sync.RWMutex
	ProducerClient 	sarama.AsyncProducer
	Version 		sarama.KafkaVersion
	Brokers 		string
	Topics 			string
	start 			time.Time
}

func (k *ProducerController)Close() {
	k.ProducerClient.Close()
}

func (k *ProducerController)TestSend(test string) {
	start := time.Now()
	oneTopic := strings.Split(k.Topics, ",")[0]

	message := &sarama.ProducerMessage{
		Topic:oneTopic,
		Value:sarama.StringEncoder(test),
	}
	k.ProducerClient.Input() <- message
	glog.V(5).Infof("send %s to kafka spend %v", test, time.Since(start))
}

func (k *ProducerController)SubscribeErrors(stop chan interface{}) {
	for {
		select {
		case <-stop:
			return
		case err := <-k.ProducerClient.Errors():
			glog.V(2).Infof("producer client error: %v", err)
		}
	}
}

func (k *ProducerController)getSendTopic() string {
	return strings.Split(k.Topics, ",")[0]
}

func (k *ProducerController)sendOneReq(req SamplesRequest) uint64 {
	start := time.Now()
	sendTopic := k.getSendTopic()

	b, err := json.Marshal(req)

	if err != nil {
		glog.Errorf("marshal request %v failed: %v", req, err)
		return uint64(len(b))
	}
	message := &sarama.ProducerMessage{
		Topic:sendTopic,
		Value:sarama.StringEncoder(string(b)),
	}
	k.ProducerClient.Input() <- message

	glog.V(5).Infof("send write request to kafka spend %v", time.Since(start))

	return uint64(len(b))
}

func (k *ProducerController)BatchSend(samples model.Samples) {
	reqs := make([]SamplesRequest, 0, len(samples))
	for _, s := range samples {
		v := float64(s.Value)
		metric := TagValue(s.Metric[model.MetricNameLabel])
		reqs = append(reqs, SamplesRequest{
			Metric:    metric,
			Timestamp: s.Timestamp.Unix(),
			Value:     v,
			Tags:      tagsFromMetric(s.Metric),
		})
	}

	var sendBytes uint64
	for _, req := range reqs {
		send := k.sendOneReq(req)
		sendBytes += send
	}

	glog.V(3).Infof("send %d(%.2f KB) samples to kafka %s - %s",
		len(samples), float64(sendBytes)/1024, k.Brokers, k.getSendTopic())
}

var MyController *ProducerController

func NewProducerController(brokers, topics, version string) (*ProducerController, error) {
	kafkaVersion, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		return nil, err
	}

	config := sarama.NewConfig()
	config.Version = kafkaVersion

	ProducerClient, err := sarama.NewAsyncProducer(strings.Split(brokers, ","), config)
	if err != nil {
		return nil, err
	}

	MyController = &ProducerController{
		Version:kafkaVersion,
		Brokers:brokers,
		ProducerClient:ProducerClient,
		Topics:topics,
		start:time.Now(),
	}

	return MyController, nil
}