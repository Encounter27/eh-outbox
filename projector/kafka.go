package projector

import (
	"context"
	"encoding/json"

	outbox "github.com/Encounter27/eh-outbox"
	"github.com/looplab/eventhorizon/repo/mongodb"
	kafka "github.com/segmentio/kafka-go"
)

// KafkaProjector is concrete implementation of IProjector for kafka
type KafkaProjector struct {
	name       string
	id         int32
	offsetRepo *mongodb.Repo
	outBoxRepo *mongodb.Repo
	Writer     *kafka.Writer
	ec         outbox.IConverter
}

// RegisterKafkaProjector is to register a kafka projector, which will publish outbox events to kafka
func RegisterKafkaProjector(name string, offsetRepo *mongodb.Repo, outboxRepo *mongodb.Repo, writer *kafka.Writer, c outbox.IConverter) {
	kafkaProjector := new(KafkaProjector)

	kafkaProjector.name = name
	kafkaProjector.AssignProjectorID(offsetRepo)
	kafkaProjector.offsetRepo = offsetRepo
	kafkaProjector.outBoxRepo = outboxRepo
	kafkaProjector.Writer = writer

	kafkaProjector.ec = c
	if c == nil {
		kafkaProjector.ec = new(outbox.DefaultConverter)
	}

	outbox.GetReadProjectorGroup().RegisterProjector(kafkaProjector)
}

// AssignProjectorID is the implemention of IProjector interface
func (p *KafkaProjector) AssignProjectorID(offsetRepo *mongodb.Repo) {
	p.id = outbox.AssignProjectorID(p.name, offsetRepo)
}

// Name is the implemention of IProjector interface
func (p *KafkaProjector) Name() string {
	return p.name
}

// Bit is the implemention of IProjector interface
func (p *KafkaProjector) Bit() int32 {
	return p.id
}

// OutboxRepo is the implemention of IProjector interface
func (p *KafkaProjector) OutboxRepo() *mongodb.Repo {
	return p.outBoxRepo
}

// OffsetRepo is the implemention of IProjector interface
func (p *KafkaProjector) OffsetRepo() *mongodb.Repo {
	return p.offsetRepo
}

// Converter is the implemention of IProjector interface
func (p *KafkaProjector) Converter() outbox.IConverter {
	return p.ec
}

// WriteToReadside is the implemention of IProjector interface
// TODO: Need ckt breaker if kafka is not available
func (p *KafkaProjector) WriteToReadside(ctx context.Context, key string, data interface{}) error {
	if bytes, err := json.Marshal(data); err == nil {
		return p.Writer.WriteMessages(ctx,
			kafka.Message{
				Key:   []byte(key),
				Value: []byte(bytes),
			},
		)
	} else {
		return err
	}
}
