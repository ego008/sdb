# sdb
A Leveldb wrapper that allows easy store hash, zset data, base on [goleveldb](https://github.com/syndtr/goleveldb)

## Example

```
db, _ := sdb.Open("testdb", nil)

db.Hset("name", "k", "v")
db.Hget("name", "k")
db.Hdel("name", "k")
db.Hincr("name", "k", 3)
db.Hscan("name", nil, 10)
db.Hrscan("name", nil, 10)

db.Zset("name", "k", 1)
db.Zget("name", "k")
db.Zdel("name", "k")
db.Zincr("name", "k", 3)
db.Zscan("name", nil, 10)
db.Zrscan("name", nil, 10)
```

[See more](https://github.com/ego008/sdb/tree/master/example)

## Users

- youBBS https://www.youbbs.org/
