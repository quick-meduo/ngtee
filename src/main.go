package main

import (
	"context"
	"flag"
	"github.com/apache/pulsar-client-go/pulsar"
	"io"
	"log"
	"os"
	"time"
)

type PulsarWriter struct {
	client pulsar.Client
	producer pulsar.Producer
}

func NewPulsarWriter(uri string,topic string) *PulsarWriter {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               uri,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})

	if err == nil {
		producer, err := client.CreateProducer(pulsar.ProducerOptions{
			Topic: topic,
			DisableBatching: true,
		})

		if err == nil {
			writer := &PulsarWriter {
				client: client,
				producer: producer,
			}
			return writer
		}
	}
	return nil
}

func (w *PulsarWriter) Close(){
   w.client.Close();
   w.producer.Close();
}

func (w *PulsarWriter) Write(p []byte) (n int, err error) {
	log.Printf("$$$ %s",string(p))
	_, err = w.producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: p,
	})
	n = len(p);
	return
}

func main() {
	topic := flag.String("topic", "non-persistent://public/default/demo", "specify topic name")
	uri := flag.String("uri", "pulsar://localhost:6650", "specify pulsar uri")
	// Enable command-line parsing
	flag.Parse()


	writer := NewPulsarWriter(*uri,*topic)

	if writer == nil {
		log.Fatalf("Could not instantiate writer")
		return
	}

	defer writer.Close()

	r := io.TeeReader(os.Stdin, writer)

	// Everything read from r will be copied to stdout.
	io.ReadAll(r)
}
