package binance

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/asaskevich/EventBus"
	"github.com/gorilla/websocket"
)

var bSubscribeCnt = 0

// WsClient struct define
type WsClient struct {
	conn   *websocket.Conn
	connMu sync.RWMutex
	stopCh chan struct{}
	evBus  EventBus.Bus

	URL    string
	errLog *log.Logger
}

type subscriptionCmd struct {
	Method string      `json:"method,omitempty"`
	Params interface{} `json:"params,omitempty"`
	ID     int         `json:"id,omitempty"`
}

type subscriptionRsp map[string]interface{}

// DepthSubscription interface for export
type DepthSubscription interface {
	Chan() <-chan *WsDepthEvent
	Close()
}

type depthSubscription struct {
	ch          <-chan *WsDepthEvent
	onEvent     func(ob *WsDepthEvent)
	unsubscribe func()
}

func (s *depthSubscription) Chan() <-chan *WsDepthEvent {
	return s.ch
}

func (s *depthSubscription) Close() {
	s.unsubscribe()
}

func toEventTopic(topic interface{}, params interface{}) string {
	s, _ := json.Marshal([]interface{}{
		topic,
		params,
	})

	return string(s)
}

// NewWsClient returns a websocket client.
func NewWsClient(l *log.Logger) (c *WsClient, err error) {
	c = &WsClient{
		stopCh: make(chan struct{}),
		evBus:  EventBus.New(),
		URL:    baseURL,
		errLog: l,
	}

	d := &websocket.Dialer{
		Subprotocols:    []string{"p1", "p2"},
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		Proxy:           http.ProxyFromEnvironment,
	}

	if c.conn, _, err = d.Dial(c.URL, nil); err != nil {
		return nil, err
	}

	go c.handleResponse()

	return
}

// SubscribeDepth Subscribe a market depth
func (w *WsClient) SubscribeDepth(market string, ch chan *WsDepthEvent) (DepthSubscription, error) {
	handler := func(ev *WsDepthEvent) {
		ch <- ev
	}

	unsubscriber, err := w.subscribeChannel("depth", []string{market}, handler)
	if err != nil {
		return nil, err
	}

	return &depthSubscription{
		ch:      ch,
		onEvent: handler,
		unsubscribe: func() {
			unsubscriber()

			if func() bool {
				select {
				case <-ch:
					return false
				default:
				}

				return true
			}() {
				close(ch)
			}
		},
	}, nil
}

func (w *WsClient) subscribeChannel(s string, params []string, handler interface{}) (func(), error) {
	bSubscribeCnt++
	req := &subscriptionCmd{
		Method: "SUBSCRIBE",
		Params: params,
		ID:     bSubscribeCnt,
	}

	topic := toEventTopic(s, params)
	if err := w.evBus.SubscribeAsync(topic, handler, true); err != nil {
		return nil, err
	}

	unsubscriber := func() {
		w.evBus.Unsubscribe(topic, handler)
	}

	return unsubscriber, w.sendReq(req)
}

func (w *WsClient) sendReq(msg interface{}) error {
	w.connMu.Lock()
	defer w.connMu.Unlock()

	return w.conn.WriteJSON(msg)
}

func (w *WsClient) readRsp(msg interface{}) error {
	w.connMu.RLock()
	defer w.connMu.RUnlock()

	return w.conn.ReadJSON(msg)
}

func (w *WsClient) handleResponse() {
	errCh := make(chan error, 1)
	for {
		resp := subscriptionRsp{}

		select {
		case errCh <- w.readRsp(&resp):
			if err := <-errCh; err != nil {
				w.errLog.Printf("Failed to read JSON, %v\n", err)
				continue
			}

			w.procResponse(resp)
		case <-w.stopCh:
			return
		}
	}
}

func (w *WsClient) procResponse(resp subscriptionRsp) {
	// TODO resp["info"]
	if resp["id"] != nil {

	}

	switch resp["e"] {
	case "depthUpdate":
		ev := &WsDepthEvent{}

		// decode
		if err := mapStruct(resp, &ev); err != nil {
			w.errLog.Println("Failed to decode depth response", err)
			return
		}

		// Publish to eventbus then to channel
		topic := toEventTopic("depth", []string{
			ev.Symbol,
		})
		go w.evBus.Publish(topic, ev)
	default:
		b, _ := json.Marshal(resp)
		w.errLog.Println("Unhandled message", string(b))
	}
}

// Close WsClient
func (w *WsClient) Close() {
	w.stopCh <- struct{}{}
	w.conn.Close()
}
