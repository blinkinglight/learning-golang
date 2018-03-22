package main

import (
	"encoding/json"
	"fmt"
	"github.com/asaskevich/govalidator"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

type Response struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Success bool              `json:"success"`
}

type Post struct {
	Title   string `valid:"required,ascii,length(2|10)" json:"title"`
	Message string `valid:"required,ascii" json:"message"`
}

func main() {

	r := mux.NewRouter()
	pr := r.PathPrefix("/v1").Methods("POST", "PUT", "DELETE").Subrouter()
	gr := r.PathPrefix("/v1").Methods("GET").Subrouter()

	_ = gr

	pr.HandleFunc("/posts", hPostPosts)

	log.Fatal(http.ListenAndServe(":9090", wrapper(r)))
}

func wrapper(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("panic: %v", r.URL.String())
			}
		}()

		if checkToken(w, r) {
			h.ServeHTTP(w, r)
		}

		log.Printf("%v %v", r.Method, r.URL.String())

	})
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

func checkToken(w http.ResponseWriter, r *http.Request) bool {
	token := r.URL.Query().Get("token")

	if r.Header.Get("X-Access-Token") != "" {
		token = r.Header.Get("X-Access-Token")
	}

	//TODO: implement
	if token == "morningstar" {
		return true
	}

	http.Error(w, "Unauthorized", http.StatusUnauthorized)

	return false
}

// http handlers

func hPostPosts(w http.ResponseWriter, r *http.Request) {

	// log.Printf("%v %v", r.Method, r.URL.String())

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
	jsonResponse(w, true, nil)
	return

}

// MIT License
