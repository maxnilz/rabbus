package rabbus

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	amqpWrap "github.com/maxnilz/rabbus/internal/amqp"

	"github.com/maxnilz/rabbus/pkg/retry"
	"github.com/sony/gobreaker"
	"github.com/streadway/amqp"
)

const (
	// Transient means higher throughput but messages will not be restored on broker restart.
	Transient uint8 = 1
	// Persistent messages will be restored to durable queues and lost on non-durable queues during server restart.
	Persistent uint8 = 2
	// ContentTypeJSON define json content type.
	ContentTypeJSON = "application/json"
	// ContentTypePlain define plain text content type.
	ContentTypePlain = "plain/text"
	// ExchangeDirect indicates the exchange is of direct type.
	ExchangeDirect = "direct"
	// ExchangeFanout indicates the exchange is of fanout type.
	ExchangeFanout = "fanout"
	// ExchangeTopic indicates the exchange is of topic type.
	ExchangeTopic = "topic"

	contentEncoding = "UTF-8"
)

type (
	// OnStateChangeFunc is the callback function when circuit breaker state changes.
	OnStateChangeFunc func(name, from, to string)

	// Message carries fields for sending messages.
	Message struct {
		// Exchange the exchange name.
		Exchange string
		// Kind the exchange type.
		Kind string
		// Key the routing key name.
		Key string
		// Payload the message payload.
		Payload []byte
		// DeliveryMode indicates if the is Persistent or Transient.
		DeliveryMode uint8
		// ContentType the message content-type.
		ContentType string
		// Headers the message application headers
		Headers map[string]interface{}
		// ContentEncoding the message encoding.
		ContentEncoding string
	}

	// ListenConfig carries fields for listening messages.
	ListenConfig struct {
		// Exchange the exchange name.
		Exchange string
		// Kind the exchange type.
		Kind string
		// Key the routing key name.
		Key string
		// PassiveExchange determines a passive exchange connection it uses
		// amqp's ExchangeDeclarePassive instead the default ExchangeDeclare
		PassiveExchange bool
		// Queue the queue name
		Queue string
		// DeclareArgs is a list of arguments accepted for when declaring the queue.
		// See https://www.rabbitmq.com/queues.html#optional-arguments for more info.
		DeclareArgs *DeclareArgs
		// BindArgs is a list of arguments accepted for when binding the exchange to the queue
		BindArgs *BindArgs
	}

	// Delivery wraps amqp.Delivery struct
	Delivery struct {
		amqp.Delivery
	}

	// Rabbus interpret (implement) Rabbus interface definition
	Rabbus struct {
		AMQP
		Logger
		mu         sync.RWMutex
		m          map[string]muxEntry
		breaker    *gobreaker.CircuitBreaker
		emit       chan Message
		emitErr    chan error
		emitOk     chan struct{}
		reconn     chan struct{}
		exDeclared map[string]struct{}
		config
		conDeclared int // conDeclared is a counter for the declared consumers

		closed chan struct{}
	}

	// AMQP exposes a interface for interacting with AMQP broker
	AMQP interface {
		// Publish wraps amqp.Publish method
		Publish(exchange, key string, opts amqp.Publishing) error
		// CreateConsumer creates a amqp consumer
		CreateConsumer(exchange, key, kind, queue string, durable bool, declareArgs, bindArgs amqp.Table) (<-chan amqp.Delivery, error)
		// WithExchange creates a amqp exchange
		WithExchange(exchange, kind string, durable bool) error
		// WithQos wrapper over amqp.Qos method
		WithQos(count, size int, global bool) error
		// NotifyClose wrapper over notifyClose method
		NotifyClose(c chan *amqp.Error) chan *amqp.Error
		// Close closes the running amqp connection and channel
		Close() error
	}

	// Emitter exposes a interface for publishing messages to AMQP broker
	Emitter interface {
		// EmitAsync emits a message to RabbitMQ, but does not wait for the response from broker.
		EmitAsync() chan<- Message
		// EmitErr returns an error if encoding payload fails, or if after circuit breaker is open or retries attempts exceed.
		EmitErr() <-chan error
		// EmitOk returns true when the message was sent.
		EmitOk() <-chan struct{}
		// EmitSync emits a message to RabbitMQ and wait the response from broker.
		EmitSync(msg *Message) error
	}

	// Logger expose a log interface for the internal logs
	Logger interface {
		Error(args ...interface{})
	}

	config struct {
		dsn               string
		durable           bool
		isExchangePassive bool
		// The size of channel pools,
		// If the size zero or less than zero, use the default configuration of underlying layer.
		chPoolSize int
		retryCfg
		breaker
		qos
	}

	retryCfg struct {
		attempts              int
		sleep, reconnectSleep time.Duration
	}

	breaker struct {
		interval, timeout time.Duration
		threshold         uint32
		onStateChange     OnStateChangeFunc
	}

	qos struct {
		prefetchCount, prefetchSize int
		global                      bool
	}

	defaultLogger struct{}
)

func (lc ListenConfig) validate() error {
	if lc.Exchange == "" {
		return ErrMissingExchange
	}

	if lc.Kind == "" {
		return ErrMissingKind
	}

	if lc.Queue == "" {
		return ErrMissingQueue
	}

	return nil
}

func (l defaultLogger) Error(args ...interface{}) {
	log.Println(args...)
}

// New returns a new Rabbus configured with the
// variables from the config parameter, or returning an non-nil err
// if an error occurred while creating connection and channel.
func New(dsn string, options ...Option) (*Rabbus, error) {
	r := &Rabbus{
		emit:       make(chan Message),
		emitErr:    make(chan error),
		emitOk:     make(chan struct{}),
		reconn:     make(chan struct{}),
		exDeclared: make(map[string]struct{}),
		closed:     make(chan struct{}),
	}

	for _, o := range options {
		if err := o(r); err != nil {
			return nil, err
		}
	}

	if r.AMQP == nil {
		amqpWrapper, err := amqpWrap.New(dsn, r.config.isExchangePassive, r.config.chPoolSize)
		if err != nil {
			return nil, err
		}
		r.AMQP = amqpWrapper
	}

	if r.Logger == nil {
		r.Logger = defaultLogger{}
	}

	if err := r.WithQos(
		r.config.qos.prefetchCount,
		r.config.qos.prefetchSize,
		r.config.qos.global,
	); err != nil {
		return nil, err
	}

	r.config.dsn = dsn
	r.breaker = gobreaker.NewCircuitBreaker(newBreakerSettings(r.config))

	// Setup a background routine for the event of channel close & async emit.
	go r.run()

	return r, nil
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as RabbitMQ bus handlers. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
type HandlerFunc func(ctx context.Context, message ConsumerMessage)

// ServeConsumerMessage calls f(w, r).
func (f HandlerFunc) ServeConsumerMessage(ctx context.Context, message ConsumerMessage) {
	f(ctx, message)
}

type Handler interface {
	ServeConsumerMessage(ctx context.Context, message ConsumerMessage)
}

type muxEntry struct {
	h Handler
	c ListenConfig
}

// HandleFunc registers the handler function for the given q.
func (r *Rabbus) HandleFunc(c ListenConfig, handler func(ctx context.Context, message ConsumerMessage)) {
	if handler == nil {
		panic(ErrMissingHandler)
	}
	r.Handle(c, HandlerFunc(handler))
}

// Handle registers the handler for the given q.
// If a handler already exists for q, Handle panics.
func (r *Rabbus) Handle(c ListenConfig, handler Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := c.validate(); err != nil {
		panic(err)
	}
	if handler == nil {
		panic(ErrMissingHandler)
	}

	if _, exist := r.m[c.Queue]; exist {
		panic(errors.New("multiple registrations for " + c.Queue))
	}

	if r.m == nil {
		r.m = make(map[string]muxEntry)
	}
	e := muxEntry{h: handler, c: c}
	r.m[c.Queue] = e
}

func (r *Rabbus) ListenAndServe(ctx context.Context) error {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
	}

	errchan := make(chan error)
	for _, entry := range r.m {
		go func(c ListenConfig) {

			errchan <- r.listenAndServe(ctx, c)

		}(entry.c)
	}

	var errs []error

	wg := sync.WaitGroup{}
	wg.Add(len(r.m))
	go func() {
		for err := range errchan {
			if err != nil {
				r.Error("rabbus:", err)
				errs = append(errs, err)
			}
			wg.Done()
		}
	}()
	wg.Wait()

	if len(errs) != 0 {
		return errors.New("rabbus:listen and serve failed, check rabbus logs for more details")
	}

	return nil
}

// NotFound record an error for the not founded handler.
func NotFound(_ context.Context, message ConsumerMessage) {
	// TODO record an error for the not founded handler
}

// NotFoundHandler returns a simple not found handler.
func NotFoundHandler() Handler { return HandlerFunc(NotFound) }

// ServeConsumerMessage dispatches the message to the handler whose
// q most closely matches the message's routing key.
func (r *Rabbus) ServeConsumerMessage(ctx context.Context, q ListenConfig, message ConsumerMessage) {
	h, _ := r.handler(q)
	if h == nil {
		h = NotFoundHandler()
	}
	h.ServeConsumerMessage(ctx, message)
}

// EmitAsync emits a message to RabbitMQ, but does not wait for the response from broker.
func (r *Rabbus) EmitAsync() chan<- Message { return r.emit }

// EmitErr returns an error if encoding payload fails, or if after circuit breaker is open or retries attempts exceed.
func (r *Rabbus) EmitErr() <-chan error { return r.emitErr }

// EmitOk returns true when the message was sent.
func (r *Rabbus) EmitOk() <-chan struct{} { return r.emitOk }

// EmitSync emits a message to RabbitMQ and wait the response from broker.
func (r *Rabbus) EmitSync(msg *Message) error {
	return r.produce(msg)
}

// Listen to a message from RabbitMQ, returns
// an error if exchange, queue name and function handler not passed or if an error occurred while creating
// amqp consumer.
func (r *Rabbus) Listen(c ListenConfig) (chan ConsumerMessage, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}

	if c.DeclareArgs == nil {
		c.DeclareArgs = NewDeclareArgs()
	}

	if c.BindArgs == nil {
		c.BindArgs = NewBindArgs()
	}

	msgs, err := r.CreateConsumer(c.Exchange, c.Key, c.Kind, c.Queue, r.config.durable, c.DeclareArgs.args, c.BindArgs.args)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	r.conDeclared++ // increase the declared consumers counter
	r.exDeclared[c.Exchange] = struct{}{}
	r.mu.Unlock()

	messages := make(chan ConsumerMessage, 256)
	go r.wrapMessage(c, msgs, messages)
	go r.listenReconnect(c, messages)

	return messages, nil
}

// Close channels and attempt to close channel and connection.
func (r *Rabbus) Close() error {
	err := r.AMQP.Close()
	close(r.emit)
	close(r.emitOk)
	close(r.emitErr)
	close(r.reconn)
	close(r.closed)
	return err
}

func (r *Rabbus) produce(m *Message) error {
	if _, ok := r.exDeclared[m.Exchange]; !ok {
		if err := r.WithExchange(m.Exchange, m.Kind, r.config.durable); err != nil {
			return err
		}
		r.exDeclared[m.Exchange] = struct{}{}
	}

	if m.ContentType == "" {
		m.ContentType = ContentTypeJSON
	}

	if m.DeliveryMode == 0 {
		m.DeliveryMode = Persistent
	}

	if m.ContentEncoding == "" {
		m.ContentEncoding = contentEncoding
	}

	opts := amqp.Publishing{
		Headers:         amqp.Table(m.Headers),
		ContentType:     m.ContentType,
		ContentEncoding: m.ContentEncoding,
		DeliveryMode:    m.DeliveryMode,
		Timestamp:       time.Now(),
		Body:            m.Payload,
	}

	if _, err := r.breaker.Execute(func() (interface{}, error) {
		return nil, retry.Do(func() error {
			return r.Publish(m.Exchange, m.Key, opts)
		}, r.config.retryCfg.attempts, r.config.retryCfg.sleep)
	}); err != nil {
		return err
	}

	return nil
}

func (r *Rabbus) produceAsync(m Message) {
	if _, ok := r.exDeclared[m.Exchange]; !ok {
		if err := r.WithExchange(m.Exchange, m.Kind, r.config.durable); err != nil {
			r.emitErr <- err
			return
		}
		r.exDeclared[m.Exchange] = struct{}{}
	}

	if m.ContentType == "" {
		m.ContentType = ContentTypeJSON
	}

	if m.DeliveryMode == 0 {
		m.DeliveryMode = Persistent
	}

	if m.ContentEncoding == "" {
		m.ContentEncoding = contentEncoding
	}

	opts := amqp.Publishing{
		Headers:         amqp.Table(m.Headers),
		ContentType:     m.ContentType,
		ContentEncoding: m.ContentEncoding,
		DeliveryMode:    m.DeliveryMode,
		Timestamp:       time.Now(),
		Body:            m.Payload,
	}

	if _, err := r.breaker.Execute(func() (interface{}, error) {
		return nil, retry.Do(func() error {
			return r.Publish(m.Exchange, m.Key, opts)
		}, r.config.retryCfg.attempts, r.config.retryCfg.sleep)
	}); err != nil {
		r.emitErr <- err
		return
	}

	r.emitOk <- struct{}{}
}

func (r *Rabbus) wrapMessage(c ListenConfig, sourceChan <-chan amqp.Delivery, targetChan chan ConsumerMessage) {
	for m := range sourceChan {
		targetChan <- newConsumerMessage(m)
	}
}

func (r *Rabbus) handleAMQPClose(err error) {
	for {
		time.Sleep(time.Second)
		aw, err := amqpWrap.New(r.config.dsn, r.config.isExchangePassive, r.config.chPoolSize)
		if err != nil {
			continue
		}

		r.mu.Lock()
		r.AMQP = aw
		r.mu.Unlock()

		if err := r.WithQos(
			r.config.qos.prefetchCount,
			r.config.qos.prefetchSize,
			r.config.qos.global,
		); err != nil {
			r.AMQP.Close()
			continue
		}

		for i := 1; i <= r.conDeclared; i++ {
			r.reconn <- struct{}{}
		}
		break
	}
}

func (r *Rabbus) listenReconnect(c ListenConfig, messages chan ConsumerMessage) {
	for range r.reconn {
		msgs, err := r.CreateConsumer(c.Exchange, c.Key, c.Kind, c.Queue, r.config.durable, c.DeclareArgs.args, c.BindArgs.args)
		if err != nil {
			continue
		}

		go r.wrapMessage(c, msgs, messages)
		go r.listenReconnect(c, messages)
		break
	}
}

// Find a handler on a handler map given a queue.
func (r *Rabbus) match(q ListenConfig) (h Handler, queue ListenConfig) {
	// Check for exact match first.
	v, ok := r.m[q.Queue]
	if ok {
		return v.h, v.c
	}

	return nil, q
}

func (r *Rabbus) handler(q ListenConfig) (h Handler, queue ListenConfig) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.match(q)
}

func (r *Rabbus) listenAndServe(ctx context.Context, c ListenConfig) error {
	if c.DeclareArgs == nil {
		c.DeclareArgs = NewDeclareArgs()
	}

	if c.BindArgs == nil {
		c.BindArgs = NewBindArgs()
	}

	msgs, err := r.CreateConsumer(c.Exchange, c.Key, c.Kind, c.Queue, r.config.durable, c.DeclareArgs.args, c.BindArgs.args)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.conDeclared++ // increase the declared consumers counter
	r.exDeclared[c.Exchange] = struct{}{}
	r.mu.Unlock()

	messages := make(chan ConsumerMessage, 256)
	go r.wrapMessage(c, msgs, messages)
	go r.listenReconnect(c, messages)

	for {
		select {
		case m, ok := <-messages:
			if !ok {
				return nil
			}
			r.ServeConsumerMessage(ctx, c, m)
		case <-ctx.Done():
			return ctx.Err()
		case <-r.closed:
			return nil
		}
	}
}

// run starts rabbus channels for emitting and listening for amqp connection close
func (r *Rabbus) run() {
	notifyClose := r.NotifyClose(make(chan *amqp.Error))

	for {
		select {
		case m, ok := <-r.emit:
			if !ok {
				return
			}

			r.produceAsync(m)

		case err := <-notifyClose:
			if err == nil {
				// "… on a graceful close, no error will be sent."
				return
			}

			r.handleAMQPClose(err)

			// We have reconnected, so we need a new NotifyClose again.
			notifyClose = r.NotifyClose(make(chan *amqp.Error))
		}
	}
}

func newBreakerSettings(c config) gobreaker.Settings {
	s := gobreaker.Settings{}
	s.Name = "rabbus-circuit-breaker"
	s.Interval = c.breaker.interval
	s.Timeout = c.breaker.timeout
	s.ReadyToTrip = func(counts gobreaker.Counts) bool {
		return counts.ConsecutiveFailures > c.breaker.threshold
	}

	if c.breaker.onStateChange != nil {
		s.OnStateChange = func(name string, from gobreaker.State, to gobreaker.State) {
			c.breaker.onStateChange(name, from.String(), to.String())
		}
	}
	return s
}
