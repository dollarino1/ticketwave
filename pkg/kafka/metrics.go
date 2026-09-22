package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Outcomes of handling one message. A closed set, so the label stays bounded.
const (
	resultHandled      = "handled"       // the handler succeeded
	resultRetried      = "retried"       // one attempt failed and another will follow
	resultDeadLettered = "dead_lettered" // gave up and parked the message on the DLQ
)

// consumed counts messages by outcome. dead_lettered is the one to alert on: it is
// data the system could not process, sitting in a topic nobody reads. A rising
// retried count with no dead_lettered is a dependency having a bad time.
var consumed = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ticketwave_kafka_consumer_messages_total",
	Help: "Kafka messages by topic, consumer group and outcome.",
}, []string{"topic", "group", "result"})

func (c *Consumer) count(topic, result string) {
	consumed.WithLabelValues(topic, c.cfg.GroupID, result).Inc()
}

// initMetrics creates every outcome's series at zero for this consumer.
//
// A counter that has never been incremented does not exist, so dashboards show "no
// data" instead of 0, and, worse, rate() and increase() lose the very first event:
// the series appears already at 1 and there is no earlier sample to measure a rise
// from. Creating the series up front means the first dead letter is seen as a rise
// from 0, which is exactly what the MessagesDeadLettered alert needs.
func initMetrics(topic, group string) {
	for _, result := range []string{resultHandled, resultRetried, resultDeadLettered} {
		consumed.WithLabelValues(topic, group, result)
	}
}
