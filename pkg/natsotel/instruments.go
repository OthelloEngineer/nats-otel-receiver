package natsotel

import (
	"sync"

	"github.com/nats-otel/nats-otel/pkg/natsemconv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type instruments struct {
	sentMsgsCounter     metric.Int64Counter
	consumedMsgsCounter metric.Int64Counter
	publishDuration     metric.Float64Histogram
	processDuration     metric.Float64Histogram
}

var (
	defaultInstrumentsLock sync.Mutex
	defaultInstrumentsInst *instruments
)

func newInstruments(mp metric.MeterProvider) *instruments {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(instrumentationName)

	sentCounter, _ := meter.Int64Counter(natsemconv.MetricMessagingClientSentMessages,
		metric.WithDescription("Number of messages sent by the client"),
		metric.WithUnit("{message}"))

	consumedCounter, _ := meter.Int64Counter(natsemconv.MetricMessagingClientConsumedMessages,
		metric.WithDescription("Number of messages consumed by the client"),
		metric.WithUnit("{message}"))

	pubDuration, _ := meter.Float64Histogram(natsemconv.MetricMessagingClientOpDuration,
		metric.WithDescription("Duration of messaging publish operations"),
		metric.WithUnit("s"))

	procDuration, _ := meter.Float64Histogram(natsemconv.MetricMessagingProcessDuration,
		metric.WithDescription("Duration of processing consumed messages"),
		metric.WithUnit("s"))

	return &instruments{
		sentMsgsCounter:     sentCounter,
		consumedMsgsCounter: consumedCounter,
		publishDuration:     pubDuration,
		processDuration:     procDuration,
	}
}

func getDefaultInstruments() *instruments {
	defaultInstrumentsLock.Lock()
	defer defaultInstrumentsLock.Unlock()
	if defaultInstrumentsInst == nil {
		defaultInstrumentsInst = newInstruments(otel.GetMeterProvider())
	}
	return defaultInstrumentsInst
}
