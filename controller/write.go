package controller

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/prompb"
	"github.com/golang/glog"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/sak0/prometheus-to-kafka/pkg/kafka"
)

func protoToSamples(req *prompb.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.Timestamp),
			})
		}
	}
	return samples
}

func WriteToKafka(c *gin.Context) {
	compressed, err := c.GetRawData()
	if err != nil {
		glog.Errorf("read error: %v", err)
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		glog.Errorf("decode error: %v", err)
		c.JSON(http.StatusBadRequest, err.Error())
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		glog.Errorf("unmarshal error: %v", err)
		c.JSON(http.StatusBadRequest, err.Error())
		return
	}
	//glog.V(5).Infof("main.go Handle Write request body: %v\n", req)

	samples := protoToSamples(&req)
	glog.V(5).Infof("main.go Handle Write samples: %v\n", samples)

	kafka.MyController.BatchSend(samples)
}