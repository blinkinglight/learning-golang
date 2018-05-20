package main

import (
	"fmt"
	"github.com/google/btree"
	"sort"
	"testing"
)

type Item struct {
	ID int
}

func (i Item) Less(item btree.Item) bool {
	return i.ID < item.(Item).ID
}

type Items struct {
	Items []Item
	Flte  int
	Fgte  int
	Lte   int
	Gte   int
}

func (i Items) Len() int {
	return len(i.Items)
}

func (a Items) Swap(i, j int) {
	a.Items[i], a.Items[j] = a.Items[j], a.Items[i]
}

func (a Items) Less(i, j int) bool {
	if a.Items[i].ID >= a.Fgte {
		if gte >= a.Items[i].ID {
			gte = a.Items[i].ID
		}
	}
	if a.Items[i].ID <= a.Flte {
		if lte <= a.Items[i].ID {
			lte = a.Items[i].ID
		}
	}
	return a.Items[i].ID < a.Items[j].ID
}

var gte int = 0
var lte int = 0

func BenchmarkSimpleSort(b *testing.B) {

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var items Items
		items.Fgte = 10
		items.Flte = 50
		for i := 0; i <= 10000; i++ {
			if i%3 == 0 {
				items.Items = append(items.Items, Item{i})
			}
		}
		sort.Sort(items)
		if i%10000 == 0 {
			fmt.Printf("%v %v %v %v\n", items.Fgte, gte, items.Flte, lte)
		}
	}
}

var blte int = 0

func BenchmarkBtreeSort(b *testing.B) {

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr := btree.New(16)
		for i := 0; i <= 10000; i++ {
			if i%3 == 0 {
				item := Item{i}
				tr.ReplaceOrInsert(item)
			}
		}
		tr.DescendLessOrEqual(Item{50}, btree.ItemIterator(func(b btree.Item) bool {
			blte = b.(Item).ID
			return false
		}))
		if i%10000 == 0 {
			fmt.Printf("%v %v\n", 50, blte)
		}
	}
}
