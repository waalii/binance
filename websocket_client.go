package binance

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/asaskevich/EventBus"
	"github.com/gorilla/websocket"
)

// WsClient struct define
type WsClient struct {
	conn    *websocket.Conn
	connMu  sync.RWMutex
	stopCh  chan struct{}
	reStart chan struct{}
	evBus   EventBus.Bus

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
func NewWsClient(l *log.Logger, reStart chan struct{}) (c *WsClient, err error) {
	c = &WsClient{
		stopCh:  make(chan struct{}),
		reStart: reStart,
		evBus:   EventBus.New(),
		URL:     baseURL,
		errLog:  l,
	}

	d := &websocket.Dialer{
		Subprotocols:    []string{"p1", "p2"},
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		// Proxy:           http.ProxyFromEnvironment,
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
	// w.errLog.Println("Subscribe", topic)
	if err := w.evBus.SubscribeAsync(topic, handler, true); err != nil {
		return nil, err
	}

	unsubscriber := func() {
		w.evBus.Unsubscribe(topic, handler)
	}

	return unsubscriber, w.sendReq(req)
}

func (w *WsClient) sendReq(msg interface{}) (err error) {
	w.connMu.Lock()
	defer w.connMu.Unlock()

	err = w.conn.WriteJSON(msg)
	return
}

func (w *WsClient) readRsp(p *[]byte) (err error) {
	// w.connMu.RLock()
	// defer w.connMu.RUnlock()
	_, *p, err = w.conn.ReadMessage()
	return
}

func (w *WsClient) handleResponse() {
	errCh := make(chan error, 1)
	errCnt := 0
	for {
		resp := []byte{}
		select {
		case errCh <- w.readRsp(&resp):
			if err := <-errCh; err != nil {
				if errCnt++; errCnt > 5 {
					w.reStart <- struct{}{}
					return
				}
				w.errLog.Printf("Failed to read Response, %v\n", err)
				time.Sleep(6 * time.Second)
				continue
			}
			errCnt = 0
			w.procResponse(resp)
		case <-w.stopCh:
			return
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
		event.Symbol = strings.ToLower(j.Get("s").MustString())
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
			event.Symbol + "@depth",
		})
		// w.errLog.Println("Publish", topic)
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
