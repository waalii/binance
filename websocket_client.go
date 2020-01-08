package binance

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/asaskevich/EventBus"
	"github.com/gorilla/websocket"
)

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
		ReadBufferSize:  2048,
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
func (w *WsClient) SubscribeDepth(id int, market string, ch chan *WsDepthEvent) (DepthSubscription, error) {
	handler := func(ev *WsDepthEvent) {
		ch <- ev
	}

	unsubscriber, err := w.subscribeChannel(id, "depth", []string{market + "@depth"}, handler)
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

func (w *WsClient) subscribeChannel(id int, s string, params []string, handler interface{}) (func(), error) {
	req := &subscriptionCmd{
		Method: "SUBSCRIBE",
		Params: params,
		ID:     id,
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

	w.errLog.Println("sendReq", msg)
	return w.conn.WriteJSON(msg)
}

func (w *WsClient) readRsp() (messageType int, p []byte, err error) {
	w.connMu.RLock()
	defer w.connMu.RUnlock()

	return w.conn.ReadMessage()
}

func (w *WsClient) handleResponse() {
	for {
		// resp := subscriptionRsp{}

		select {

		case <-w.stopCh:
			return
		default:
			if _, message, err := w.readRsp(); err != nil {
				w.errLog.Printf("Failed to read Response, %v\n", err)
				continue
			} else {
				w.procResponse(message)
			}

		}
	}
}

func (w *WsClient) procResponse(resp []byte) {
	j, err := newJSON(resp)
	if err != nil {
		w.errLog.Println(err)
		return
	}

	switch j.Get("e").MustString() {
	case "depthUpdate":

		// mapStruct
		event := new(WsDepthEvent)
		event.Event = j.Get("e").MustString()
		event.Time = j.Get("E").MustInt64()
		event.Symbol = j.Get("s").MustString()
		event.UpdateID = j.Get("u").MustInt64()
		event.FirstUpdateID = j.Get("U").MustInt64()
		bidsLen := len(j.Get("b").MustArray())
		event.Bids = make([]Bid, bidsLen)
		for i := 0; i < bidsLen; i++ {
			item := j.Get("b").GetIndex(i)
			event.Bids[i] = Bid{
				Price:    item.GetIndex(0).MustString(),
				Quantity: item.GetIndex(1).MustString(),
			}
		}
		asksLen := len(j.Get("a").MustArray())
		event.Asks = make([]Ask, asksLen)
		for i := 0; i < asksLen; i++ {
			item := j.Get("a").GetIndex(i)
			event.Asks[i] = Ask{
				Price:    item.GetIndex(0).MustString(),
				Quantity: item.GetIndex(1).MustString(),
			}
		}
		// Publish to eventbus then to channel
		topic := toEventTopic("depth", []string{
			event.Symbol,
		})
		// w.errLog.Println("depthUpdate", event)
		go w.evBus.Publish(topic, event)
	default:
		w.errLog.Println("Unhandled message", string(resp))
	}
}

// Close WsClient
func (w *WsClient) Close() {
	w.conn.Close()
	w.stopCh <- struct{}{}
}
