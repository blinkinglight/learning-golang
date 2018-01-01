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
			_, err = ioutil.ReadAll(io.LimitReader(ts_response.Body, blen))
			if err != nil {
				eCritical("panic step 4, error: %v", err)
			}
			fmt.Printf("OK; downloaded %v bytes\n", blen)
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
