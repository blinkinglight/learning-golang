package main

import (
	"context"
	"fmt"
	"github.com/nats-io/nuid"
	"log"
	"net/http"
)

func main() {

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		r.Context().Value("msg").(*MyMessage).Set("somthing-in-method", "rsp OK")
		r.Context().Value("msg").(*MyMessage).Set("another-param", "param1 vlaue in handlefunc")
		w.Write([]byte("OK\n"))
	})

	fmt.Println(http.ListenAndServe(":10000", wrapper(mux)))
}

func wrapper(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		msg := NewMyMessage(fmt.Sprintf("%v %v", r.Method, r.URL.String()))
		msg.Set("user-agent", r.UserAgent())
		msg.Set("request-id", nuid.New().Next())

		ctx := context.WithValue(context.Background(), "msg", msg)
		r = r.WithContext(ctx)

		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("panic: %s", msg.String())
			}
		}()

		h.ServeHTTP(w, r)

		log.Println(msg.String())

	})
}

type MyMessage struct {
	Message string
	Params  map[string]interface{}
	params  []string
}

func (dm *MyMessage) Set(key string, value interface{}) {
	dm.Params[key] = value
	dm.params = append(dm.params, key)
}

func (dm *MyMessage) Get(key string) string {
	v, ok := dm.Params[key]
	if ok {
		return v.(string)
	}
	return ""
}

func (dm *MyMessage) String() string {
	msg := dm.Message
	for _, v := range dm.params {
		msg += " " + fmt.Sprintf("%s=\"%v\"", v, dm.Params[v])
	}
	return msg
}

func NewMyMessage(msg string) *MyMessage {
	dm := new(MyMessage)
	dm.Message = msg
	dm.Params = make(map[string]interface{})
	return dm
}
