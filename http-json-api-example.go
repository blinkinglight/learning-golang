package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/asaskevich/govalidator"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"strings"
)

const apiPrefix = "api"
const apiVersion = "v1"

type Response struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Success bool              `json:"success"`
	Data    interface{}       `json:"data"`
}

type Post struct {
	Title   string `valid:"required,ascii,length(2|10)" json:"title"`
	Message string `valid:"required,ascii" json:"message"`
}

var bindTo = flag.String("bind-to", ":9090", "-bind-to :port or host:port")

func main() {

	flag.Parse()

	r := mux.NewRouter()
	pr := r.PathPrefix(fmt.Sprintf("/%s/%s", apiPrefix, apiVersion)).Methods("POST", "PUT").Subrouter()
	dr := r.PathPrefix(fmt.Sprintf("/%s/%s", apiPrefix, apiVersion)).Methods("DELETE").Subrouter()
	gr := r.PathPrefix(fmt.Sprintf("/%s/%s", apiPrefix, apiVersion)).Methods("GET").Subrouter()

	// api post routes
	pr.HandleFunc("/posts", hPostPosts)
	// api delete routes
	dr.HandleFunc("/post/{id:[0-9]+}", hDeletePost)
	// api get routes
	gr.HandleFunc("/posts", hGetPosts)

	// public methods
	r.HandleFunc("/public", publicMethod)

	log.Fatal(http.ListenAndServe(*bindTo, wrapper(r)))
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

func jsonResponse(w http.ResponseWriter, success bool, errors error, data interface{}) {
	var response Response

	if errors == nil {
		errors = fmt.Errorf("")
	}

	response.Errors = govalidator.ErrorsByField(errors)
	response.Message = errors.Error()
	response.Success = success
	response.Data = data

	b, _ := json.Marshal(response)

	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

func checkToken(w http.ResponseWriter, r *http.Request) bool {
	if !strings.HasPrefix(r.URL.String(), "/"+apiPrefix) {
		return true
	}

	token := r.URL.Query().Get("token")

	if r.Header.Get("X-Access-Token") != "" {
		token = r.Header.Get("X-Access-Token")
	}

	//TODO: implement
	if isValidToken(token) {
		return true
	}

	w.Header().Set("Content-Type", "application/json")
	http.Error(w, `"Unauthorized"`, http.StatusUnauthorized)

	return false
}

// your token check method here
func isValidToken(token string) bool {
	if token == "morningstar" {
		return true
	}
	return false
}

// http handlers

func hPostPosts(w http.ResponseWriter, r *http.Request) {

	// log.Printf("%v %v", r.Method, r.URL.String())

	var post Post

	err := json.NewDecoder(r.Body).Decode(&post)

	if err != nil {
		jsonResponse(w, false, err, nil)
		return
	}

	isValid, err := govalidator.ValidateStruct(post)

	if !isValid {
		jsonResponse(w, false, err, nil)
		return
	}

	// do something
	// save to database
	// send to mq something
	// etc.
	jsonResponse(w, true, nil, nil)
	return

}

// example get posts
type Record struct {
	Title string
	Body  string
}

type Records []*Record

func hGetPosts(w http.ResponseWriter, r *http.Request) {
	records := &Records{
		&Record{"title 1", "long body 1"},
		&Record{"title 2", "long body 2"},
		&Record{"title 3", "long body 3"},
	}
	jsonResponse(w, true, nil, records)
}

// example delete post
func hDeletePost(w http.ResponseWriter, r *http.Request) {
	varz := mux.Vars(r)
	jsonResponse(w, true, nil, fmt.Sprintf("Deleting record with id: %v", varz["id"]))
}

func publicMethod(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Public method\n"))
}

// MIT License
