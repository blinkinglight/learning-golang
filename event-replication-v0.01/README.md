# golang event replication pre-alpha

this is prototype of single mysql table "id | blob" replication and self healing


id = GetUnixNano()

id = id - (id % 100 ) + shardN[0..99]


id = store(blob)

play(id, id, func(msg []byte) {})

playAndSubscribe(id, func(msg []byte) {})


// unfinished
