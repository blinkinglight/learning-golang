package main

import (
	"encoding/json"
	"github.com/asaskevich/govalidator"
	"io/ioutil"
	"log"
	"net/http"
)

type Response struct {
	Errors  map[string]string `json:"errors"`
	Success bool              `json:"success"`
}

type Post struct {
	Title   string `valid:"required,ascii" json:"title"`
	Message string `valid:"required,ascii" json:"message"`
}

func main() {
	http.HandleFunc("/v1/posts", func(w http.ResponseWriter, r *http.Request) {

		log.Printf("%v %v", r.Method, r.URL.String())

		pb, _ := ioutil.ReadAll(r.Body)
		var post Post

		json.Unmarshal(pb, &post)

		var response Response

		isValid, err := govalidator.ValidateStruct(post)

		response.Errors = govalidator.ErrorsByField(err)
		response.Success = isValid

		b, _ := json.Marshal(response)
		if !isValid {
			w.WriteHeader(400)
		} else {
			// do something
			// save to database
			// send to mq something
			// etc.
		}
		w.Write(b)
	})

	log.Fatal(http.ListenAndServe(":9090", nil))
}

