package main

import (
	"encoding/json"
	"fmt"
	"github.com/asaskevich/govalidator"
	"log"
	"net/http"
)

type Response struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Success bool              `json:"success"`
}

type Post struct {
	Title   string `valid:"required,ascii" json:"title"`
	Message string `valid:"required,ascii" json:"message"`
}

func main() {
	http.HandleFunc("/v1/posts", func(w http.ResponseWriter, r *http.Request) {

		log.Printf("%v %v", r.Method, r.URL.String())

		var post Post

		err := json.NewDecoder(r.Body).Decode(&post)

		if err != nil {
			jsonResponse(w, false, err)
			return
		}

		isValid, err := govalidator.ValidateStruct(post)

		if !isValid {
			jsonResponse(w, false, err)
			return
		}

		// do something
		// save to database
		// send to mq something
		// etc.
		jsonResponse(w, isValid, nil)
		return

	})

	log.Fatal(http.ListenAndServe(":9090", nil))
}

func jsonResponse(w http.ResponseWriter, success bool, errors error) {
	var response Response

	if errors == nil {
		errors = fmt.Errorf("")
	}

	response.Errors = govalidator.ErrorsByField(errors)
	response.Message = errors.Error()
	response.Success = success

	b, _ := json.Marshal(response)

	w.Write(b)
}
