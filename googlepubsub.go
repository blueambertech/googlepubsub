package googlepubsub

import (
	"context"
	"errors"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
)

type Message struct {
	pubsub.Message
}

type GooglePubSub struct {
	client Client
}

type Client interface {
	Subscription(id string) *pubsub.Subscription
	Topic(id string) *pubsub.Topic
}

// New creates a client and returns a pubsub handler
func New(ctx context.Context, projectID string) (*GooglePubSub, error) {
	c, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	return &GooglePubSub{client: c}, nil
}

// New uses an existing client and returns a pubsub handler
func NewWithClient(ctx context.Context, projectID string, client Client) (*GooglePubSub, error) {
	return &GooglePubSub{client: client}, nil
}

// Subscribe pulls messages from an existing subscription at a supplied refresh rate and executes the msgProc function when a message is received. The
// msgProc func is internally wrapped to Ack the message on completion. This function blocks until the cancCtx is completed/cancelled.
func (ps *GooglePubSub) Subscribe(cancCtx context.Context, subID string, refreshRate time.Duration, msgProc func(c context.Context, msgData []byte)) error {
	sub := ps.client.Subscription(subID)
	if sub == nil {
		return errors.New("could not find subscription with that ID: " + subID)
	}

	mpWrapper := func(c context.Context, msg *pubsub.Message) {
		defer msg.Ack()
		msgProc(c, msg.Data)
	}

	for {
		select {
		case <-cancCtx.Done():
			return nil
		default:
			err := sub.Receive(cancCtx, func(c context.Context, msg *pubsub.Message) {
				mpWrapper(c, msg)
			})
			if err != nil {
				log.Printf("Error pulling message: %v", err)
			}
		}
		time.Sleep(refreshRate)
	}
}

// Push pushes a message to specified topic and waits for the operation to complete
func (ps *GooglePubSub) Push(ctx context.Context, topicID, msg string) error {
	t := ps.client.Topic(topicID)
	if t == nil {
		return errors.New("could not find topic with that ID: " + topicID)
	}
	result := t.Publish(ctx, &pubsub.Message{
		Data: []byte(msg),
	})
	if _, err := result.Get(ctx); err != nil {
		return errors.New("error publishing message to topic: " + err.Error())
	}
	return nil
}
