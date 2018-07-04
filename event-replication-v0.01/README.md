# golang event replication pre-alpha

this is prototype of single mysql table "id | blob" replication 


id = GetUnixNano()

id = id - (id % 100 ) + shardN[0..99]


store(id, blob)

play(id, id)

playAndSubscribe(id, func(msg []byte) {})


