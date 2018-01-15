package main

import (
	"fmt"
	"github.com/grafov/m3u8"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

var blen int64 = 500000
var httpClient http.Client
var warnTime float64 = 1<<31 - 1
var critTime float64 = 1 << 31

func init() {
	httpClient = http.Client{}
	httpClient.Timeout = time.Duration(3) * time.Second

	if len(os.Args) == 1 {
		eCritical("%v http://abc.tl/p/t/pl.m3u8", os.Args[0])
		os.Exit(2)
	}
	if len(os.Args) > 2 {
		num, _ := strconv.Atoi(os.Args[2])
		blen = int64(num)
	}
	if len(os.Args) > 4 {
		warnTime, _ = strconv.ParseFloat(os.Args[3], 10)
		critTime, _ = strconv.ParseFloat(os.Args[4], 10)
	}
}

func main() {
	purl, err := url.Parse(os.Args[1])
	if err != nil {
		eCritical("cant parse %v, error: %v", err, os.Args[1])
	}

START:
	response, err := httpClient.Get(purl.String())

	if err != nil {
		eCritical("panic step 1, error: %v", err)
	}

	p, listType, err := m3u8.DecodeFrom(response.Body, true)
	if err != nil {
		eCritical("panic step 2, error: %v", err)
	}
	switch listType {
	case m3u8.MEDIA:
		mediapl := p.(*m3u8.MediaPlaylist)
		if len(mediapl.Segments) > 0 {
			seg := mediapl.Segments[0]
			uri, _ := purl.Parse(seg.URI)
			ts_response, err := http.Get(uri.String())
			defer ts_response.Body.Close()
			if err != nil {
				eCritical("panic step 3, error: %v", err)
			}
			start := time.Now()
			var body []byte
			// if max content len is important
			if blen > 0 {
				body, err = ioutil.ReadAll(io.LimitReader(ts_response.Body, blen))
			} else {
				body, err = ioutil.ReadAll(ts_response.Body)
			}
			bodylen := len(body)
			end := time.Now()
			if err != nil {
				eCritical("panic step 4, error: %v", err)
			}
			diff := end.Sub(start).Seconds()
			if diff > critTime {
				eCritical("response time to high: %v", diff)
			}
			if diff > warnTime {
				fmt.Printf("WARNING; response time to hight: %v\n", diff)
				os.Exit(1)
			}
			fmt.Printf("OK; downloaded %v bytes in %v seconds\n", bodylen, diff)
			os.Exit(0)
		} else {
			eCritical("found 0 segments")
		}
	case m3u8.MASTER:
		masterpl := p.(*m3u8.MasterPlaylist)
		if len(masterpl.Variants) == 0 {
			eCritical("found 0 playlists")
		}
		_url, err := purl.Parse(masterpl.Variants[0].URI)
		if err != nil {
			eCritical("cant parse url: %v, error: %v", _url, err)
		}
		purl = _url
		goto START
	}
}

func eCritical(params ...interface{}) {
	fmt.Printf("CRITICAL; "+params[0].(string)+"\n", params[1:]...)
	os.Exit(2)
}

// usage: ./check_hsl http://dom/playlist.m3u8 0 2 4
// 2 - > 2s warning response time, 4 = > 4s critical response time

