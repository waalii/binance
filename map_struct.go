package binance

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

func mapStruct(src interface{}, v interface{}) (err error) {
	pr, pw := io.Pipe()
	errCh := make(chan error, 2)
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer pw.Close()
		errCh <- json.NewEncoder(pw).Encode(src)
	}()

	go func() {
		defer wg.Done()
		errCh <- json.NewDecoder(pr).Decode(v)
	}()

	wg.Wait()
	close(errCh)

	e1 := <-errCh
	e2 := <-errCh

	if e1 != nil {
		err = fmt.Errorf("%v, %v", err, e1)
	}

	if e2 != nil {
		err = fmt.Errorf("%v, %v", err, e2)
	}

	return err
}
