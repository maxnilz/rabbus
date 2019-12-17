package amqp

import (
	"runtime"
	"sync"

	"github.com/streadway/amqp"
)

// queue a classic thread-safe queue in lock contention
type queue struct {
	sync.Mutex
	items []*amqp.Channel
}

func (q *queue) push(item *amqp.Channel) {
	q.Lock()
	defer q.Unlock()
	q.items = append(q.items, item)
}

func (q *queue) pop() *amqp.Channel {
	q.Lock()
	defer q.Unlock()
	if len(q.items) == 0 {
		return nil
	}
	item := q.items[0]
	q.items = q.items[1:]
	return item
}

func (q *queue) iterate(c chan<- interface{}) {
	for {
		datum := q.pop()
		if datum == nil {
			break
		}

		c <- datum
	}
	close(c)
}

func (q *queue) iter() <-chan interface{} {
	c := make(chan interface{})
	go q.iterate(c)
	return c
}

func newQueue(capacity int) *queue {
	return &queue{
		items: make([]*amqp.Channel, 0, capacity),
	}
}

type (
	// AMQP interpret (implement) AMQP interface definition
	AMQP struct {
		conn            *amqp.Connection
		passiveExchange bool

		// the potentially issue here is,
		// at some point, there might many concurrent accesses,
		// it will spend more time waiting for locks to be released than actually doing work.
		// When it comes to this point, consider using lock-free queue as the replacement
		chPools *queue
	}
)

// New returns a new AMQP configured, or returning an non-nil err
// if an error occurred while creating connection or channel.
func New(dsn string, pex bool, poolSize int) (*AMQP, error) {
	conn, err := createConn(dsn)
	if err != nil {
		return nil, err
	}

	if poolSize <= 0 {
		// Set the default pool size to 5 times of num of cpu
		poolSize = runtime.NumCPU() * 5
	}
	chPools := newQueue(poolSize)
	for i := 0; i < poolSize; i++ {
		ch, err := createChan(conn)
		if err != nil {
			return nil, err
		}
		chPools.push(ch)
	}

	return &AMQP{
		conn:            conn,
		chPools:         chPools,
		passiveExchange: pex,
	}, nil
}

// Publish wraps amqp.Publish method
func (ai *AMQP) Publish(exchange, key string, opts amqp.Publishing) error {
	ch := ai.chPools.pop()
	defer ai.chPools.push(ch)

	return ch.Publish(exchange, key, false, false, opts)
}

// CreateConsumer creates a amqp consumer. Most interesting declare args are:
func (ai *AMQP) CreateConsumer(exchange, key, kind, queue string, durable bool, declareArgs, bindArgs amqp.Table) (<-chan amqp.Delivery, error) {
	if err := ai.WithExchange(exchange, kind, durable); err != nil {
		return nil, err
	}

	ch := ai.chPools.pop()
	defer ai.chPools.push(ch)

	q, err := ch.QueueDeclare(queue, durable, false, false, false, declareArgs)
	if err != nil {
		return nil, err
	}

	if err := ch.QueueBind(q.Name, key, exchange, false, bindArgs); err != nil {
		return nil, err
	}

	return ch.Consume(q.Name, "", false, false, false, false, nil)
}

// WithExchange creates a amqp exchange
func (ai *AMQP) WithExchange(exchange, kind string, durable bool) error {
	ch := ai.chPools.pop()
	defer ai.chPools.push(ch)

	if ai.passiveExchange {
		return ch.ExchangeDeclarePassive(exchange, kind, durable, false, false, false, nil)
	}

	return ch.ExchangeDeclare(exchange, kind, durable, false, false, false, nil)
}

// WithQos wrapper over amqp.Qos method
func (ai *AMQP) WithQos(count, size int, global bool) error {
	ch := ai.chPools.pop()
	defer ai.chPools.push(ch)

	return ch.Qos(count, size, global)
}

// NotifyClose wrapper over notifyClose method
func (ai *AMQP) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	return ai.conn.NotifyClose(c)
}

// Close closes the running amqp connection and channel
func (ai *AMQP) Close() error {
	for v := range ai.chPools.iter() {
		ch := v.(*amqp.Channel)
		if err := ch.Close(); err != nil {
			return err
		}
	}

	if ai.conn != nil {
		return ai.conn.Close()
	}

	return nil
}

func createChan(conn *amqp.Connection) (*amqp.Channel, error) {
	return conn.Channel()
}

func createConn(dsn string) (*amqp.Connection, error) {
	return amqp.Dial(dsn)
}
