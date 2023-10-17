package googlepubsub

import (
	"context"
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
func (ps *GooglePubSub) Subscribe(cancCtx context.Context, subID string, refreshRate time.Duration, msgProc func(c context.Context, msgData []byte)) {
	sub := ps.client.Subscription(subID)

	mpWrapper := func(c context.Context, msg *pubsub.Message) {
		defer msg.Ack()
		msgProc(c, msg.Data)
	}

	for {
		select {
		case <-cancCtx.Done():
			return
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
