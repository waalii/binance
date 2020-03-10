package binance

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/asaskevich/EventBus"
	"github.com/bitly/go-simplejson"
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
	stdLog *log.Logger
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

// MiniTickerSubscription interface for export
type MiniTickerSubscription interface {
	Chan() <-chan *WsMiniMarketsStatEvent
	Close()
}

type miniTickerSubscription struct {
	ch          <-chan *WsMiniMarketsStatEvent
	onEvent     func(ob *WsMiniMarketsStatEvent)
	unsubscribe func()
}

func (s *miniTickerSubscription) Chan() <-chan *WsMiniMarketsStatEvent {
	return s.ch
}

func (s *miniTickerSubscription) Close() {
	s.unsubscribe()
}

type TickerSubscription interface {
	Chan() <-chan *WsMarketStatEvent
	Close()
}

type tickerSubscription struct {
	ch          <-chan *WsMarketStatEvent
	onEvent     func(ob *WsMarketStatEvent)
	unsubscribe func()
}

func (s *tickerSubscription) Chan() <-chan *WsMarketStatEvent {
	return s.ch
}

func (s *tickerSubscription) Close() {
	s.unsubscribe()
}

type KlineSubscription interface {
	Chan() <-chan *WsKlineEvent
	Close()
}

type klineSubscription struct {
	ch          <-chan *WsKlineEvent
	onEvent     func(ob *WsKlineEvent)
	unsubscribe func()
}

func (s *klineSubscription) Chan() <-chan *WsKlineEvent {
	return s.ch
}

func (s *klineSubscription) Close() {
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
func NewWsClient(l, e *log.Logger, reStart chan struct{}) (c *WsClient, err error) {
	c = &WsClient{
		stopCh:  make(chan struct{}),
		reStart: reStart,
		evBus:   EventBus.New(),
		URL:     baseURL,
		stdLog:  l,
		errLog:  e,
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

// SubscribeMinTick Subscribe a market depth
func (w *WsClient) SubscribeMinTick(id int, market string, ch chan *WsMiniMarketsStatEvent) (MiniTickerSubscription, error) {
	handler := func(ev *WsMiniMarketsStatEvent) {
		ch <- ev
	}

	unsubscriber, err := w.subscribeChannel(id, "miniTicker", []string{market + "@miniTicker"}, handler)
	if err != nil {
		return nil, err
	}

	return &miniTickerSubscription{
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

// SubscribeTick Subscribe a market depth
func (w *WsClient) SubscribeTick(id int, market string, ch chan *WsMarketStatEvent) (TickerSubscription, error) {
	handler := func(ev *WsMarketStatEvent) {
		ch <- ev
	}

	unsubscriber, err := w.subscribeChannel(id, "ticker", []string{market + "@ticker"}, handler)
	if err != nil {
		return nil, err
	}

	return &tickerSubscription{
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

// SubscribeKline Subscribe a market depth
func (w *WsClient) SubscribeKline(id int, market, interval string, ch chan *WsKlineEvent) (KlineSubscription, error) {
	handler := func(ev *WsKlineEvent) {
		ch <- ev
	}

	unsubscriber, err := w.subscribeChannel(id, "kline", []string{market + "@kline_" + interval}, handler)
	if err != nil {
		return nil, err
	}

	return &klineSubscription{
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

func (w *WsClient) procDepthUpdate(j *simplejson.Json) (topic string, event *WsDepthEvent) {
	event = new(WsDepthEvent)
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
	topic = toEventTopic("depth", []string{
		event.Symbol + "@depth",
	})

	return
}

func (w *WsClient) procMiniTicker(j *simplejson.Json) (topic string, event *WsMiniMarketsStatEvent) {
	event = new(WsMiniMarketsStatEvent)
	event.Event = j.Get("e").MustString()
	event.Time = j.Get("E").MustInt64()
	event.Symbol = strings.ToLower(j.Get("s").MustString())
	event.LastPrice = j.Get("c").MustString()
	event.OpenPrice = j.Get("o").MustString()
	event.HighPrice = j.Get("h").MustString()
	event.LowPrice = j.Get("l").MustString()
	event.BaseVolume = j.Get("v").MustString()
	event.QuoteVolume = j.Get("q").MustString()

	// Publish to eventbus then to channel
	topic = toEventTopic("miniTicker", []string{
		event.Symbol + "@miniTicker",
	})
	return

}

func (w *WsClient) procTicker(j *simplejson.Json) (topic string, event *WsMarketStatEvent) {
	event = new(WsMarketStatEvent)
	event.Event = j.Get("e").MustString()
	event.Time = j.Get("E").MustInt64()
	event.Symbol = strings.ToLower(j.Get("s").MustString())
	event.PriceChange = j.Get("p").MustString()
	event.PriceChangePercent = j.Get("P").MustString()
	event.WeightedAvgPrice = j.Get("w").MustString()
	event.PrevClosePrice = j.Get("x").MustString()
	event.LastPrice = j.Get("c").MustString()
	event.CloseQty = j.Get("Q").MustString()
	event.BidPrice = j.Get("b").MustString()
	event.BidQty = j.Get("B").MustString()
	event.AskPrice = j.Get("a").MustString()
	event.AskQty = j.Get("A").MustString()
	event.OpenPrice = j.Get("o").MustString()
	event.HighPrice = j.Get("h").MustString()
	event.LowPrice = j.Get("l").MustString()
	event.BaseVolume = j.Get("v").MustString()
	event.QuoteVolume = j.Get("q").MustString()
	event.OpenTime = j.Get("O").MustInt64()
	event.CloseTime = j.Get("C").MustInt64()
	event.FirstID = j.Get("F").MustInt64()
	event.LastID = j.Get("L").MustInt64()
	event.Count = j.Get("n").MustInt64()

	// Publish to eventbus then to channel
	topic = toEventTopic("ticker", []string{
		event.Symbol + "@ticker",
	})
	return
}

func (w *WsClient) procKline(j *simplejson.Json) (topic string, event *WsKlineEvent) {
	event = new(WsKlineEvent)
	event.Event = j.Get("e").MustString()
	event.Time = j.Get("E").MustInt64()
	event.Symbol = strings.ToLower(j.Get("s").MustString())
	event.Kline.StartTime = j.Get("k").Get("t").MustInt64()
	event.Kline.EndTime = j.Get("k").Get("T").MustInt64()
	event.Kline.Symbol = j.Get("k").Get("s").MustString()
	event.Kline.Interval = j.Get("k").Get("i").MustString()
	event.Kline.FirstTradeID = j.Get("k").Get("f").MustInt64()
	event.Kline.LastTradeID = j.Get("k").Get("L").MustInt64()
	event.Kline.Open = j.Get("k").Get("o").MustString()
	event.Kline.Close = j.Get("k").Get("c").MustString()
	event.Kline.High = j.Get("k").Get("h").MustString()
	event.Kline.Low = j.Get("k").Get("l").MustString()
	event.Kline.Volume = j.Get("k").Get("v").MustString()
	event.Kline.TradeNum = j.Get("k").Get("n").MustInt64()
	event.Kline.IsFinal = j.Get("k").Get("x").MustBool()
	event.Kline.QuoteVolume = j.Get("k").Get("q").MustString()
	event.Kline.ActiveBuyVolume = j.Get("k").Get("V").MustString()
	event.Kline.ActiveBuyQuoteVolume = j.Get("k").Get("Q").MustString()

	// Publish to eventbus then to channel
	topic = toEventTopic("kline", []string{
		event.Symbol + "@kline",
	})
	return

}

func (w *WsClient) procResponse(resp []byte) {
	j, err := newJSON(resp)
	if err != nil {
		w.errLog.Println(err)
		return
	}

	switch j.Get("e").MustString() {
	case "depthUpdate":
		topic, event := w.procDepthUpdate(j)
		go w.evBus.Publish(topic, event)
	case "24hrMiniTicker":
		topic, event := w.procMiniTicker(j)
		go w.evBus.Publish(topic, event)
	case "24hrTicker":
		topic, event := w.procTicker(j)
		go w.evBus.Publish(topic, event)
	case "kline":
		topic, event := w.procKline(j)
		go w.evBus.Publish(topic, event)
	default:
		if j.Get("id") != nil {
			w.stdLog.Println("success", string(resp))
		} else {
			w.errLog.Println("Unhandled message", string(resp))
		}
	}
}

// Close WsClient
func (w *WsClient) Close() {
	w.conn.Close()
	w.stopCh <- struct{}{}
}
