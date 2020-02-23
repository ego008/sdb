package main

import (
	"fmt"
	"github.com/ego008/sdb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"log"
)

func main() {
	o := &opt.Options{
		Filter: filter.NewBloomFilter(10), // 一般取10
	}
	db, err := sdb.Open("testdb", o)
	if err != nil {
		log.Fatalln(err)
	}

	vals := []interface{}{
		"my value",
		123456,
		[]byte("byte value"),
		[][]byte{[]byte("a"), []byte("b")},
		[]string{"c", "d"},
		[]int{7, 8, 9},
	}

	name := "mytest"
	key := "mykey"

	for _, v := range vals {
		err = db.Hset(name, key, v)
		if err != nil {
			log.Fatalln(err)
		}
		rs := db.Hget(name, key)
		if !rs.OK() {
			log.Fatalln(rs.State)
		}
		fmt.Println(v, rs.String(), rs.Bytes())
	}

	// notfound
	rs := db.Hget("mytest2", "mykey2")
	if !rs.NotFound() {
		log.Fatalln(rs.State)
	}
	fmt.Println(rs.State, string(rs.String()))

	// hmset + hmget
	kvs := []interface{}{"k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4", "k5", "v5", "k8", "v8"}
	err = db.Hmset(name, kvs)
	if err != nil {
		log.Fatalln(err)
	}
	rs = db.Hmget(name, [][]byte{[]byte("k1"), []byte("k2")})
	if !rs.OK() {
		log.Fatalln("not ok")
	}
	rs.KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})

	//
	fmt.Println(db.Hdel("count", "mytest"))
	fmt.Println(db.Hincr("count", "mytest", 12))
	fmt.Println(db.HgetInt("count", "mytest")) // 12

	fmt.Println(db.Hmdel(name, [][]byte{[]byte("k1"), []byte("k2")}))
	fmt.Println(db.Hget(name, "k1"))
	fmt.Println(db.Hset(name, "k1", "v11"))
	fmt.Println(db.Hget(name, "k1"))
	fmt.Println(db.HdelBucket(name))
	fmt.Println(db.Hget(name, "k1"))
	fmt.Println(db.HgetInt("count", "mytest")) // 12

	fmt.Println(db.HdelBucket(name))
	_ = db.Hmset(name, kvs)
	fmt.Println("--- Hmget ---")
	db.Hmget(name, [][]byte{[]byte("k1"), []byte("k2"), []byte("k3"), []byte("k4")}).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})
	fmt.Println("--- Hscan ---")
	db.Hscan(name, "k2", 2).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("--- Hscan 2 ---")
	db.Hscan(name, "k6", 4).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("--- Hscan 3 ---")
	db.Hscan(name, nil, 9).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("--- Hrscan ---")
	db.Hrscan(name, "k5", 2).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("--- Hrscan 2 ---")
	db.Hrscan(name, "k7", 4).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("--- Hrscan 3 ---")
	db.Hrscan(name, nil, 9).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("zet set/get")
	fmt.Println(db.Zset(name, "k1", 12))
	fmt.Println(db.Zget(name, "k1"))
	fmt.Println(db.Zincr(name, "k1", 2))
	fmt.Println(db.Zincr(name, "k1", -3))
	fmt.Println(db.Zdel(name, "k1"))
	fmt.Println(db.Zget(name, "k1"))
	fmt.Println(db.Zincr(name, "k1", 2))
	fmt.Println(db.Zincr(name, "k2", 2))
	fmt.Println(db.ZdelBucket(name))
	fmt.Println(db.Zincr(name, "k1", 2))
	fmt.Println(db.Zincr(name, "k2", 2))

	// zmset
	fmt.Println(db.Zmset(name, [][]byte{
		[]byte("k1"), sdb.I2b(1),
		[]byte("k2"), sdb.I2b(2),
		[]byte("k3"), sdb.I2b(3),
		[]byte("k4"), sdb.I2b(4),
		[]byte("k5"), sdb.I2b(5),
		[]byte("k6"), sdb.I2b(2),
		[]byte("k7"), sdb.I2b(5),
		[]byte("k8"), sdb.I2b(5),
		[]byte("k9"), sdb.I2b(5),
		[]byte("k10"), sdb.I2b(5),
		[]byte("k11"), sdb.I2b(1),
		[]byte("k12"), sdb.I2b(2),
		[]byte("k13"), sdb.I2b(3),
		[]byte("k14"), sdb.I2b(4),
		[]byte("k15"), sdb.I2b(5),
		[]byte("k16"), sdb.I2b(2),
		[]byte("k17"), sdb.I2b(5),
		[]byte("k18"), sdb.I2b(5),
		[]byte("k19"), sdb.I2b(5),
		[]byte("k20"), sdb.I2b(5),
	}))

	fmt.Println("-- Zmget")
	db.Zmget(name, [][]byte{[]byte("k2"), []byte("k3"), []byte("k4")}).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), sdb.B2i(value))
	})

	fmt.Println("-- Zscan")
	db.Zscan(name, "k2", sdb.I2b(2), 30).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), sdb.B2i(value))
	})

	fmt.Println("-- Zscan page")
	var keyStart []byte
	var scourStart []byte
	ii := 0
	for {
		rs := db.Zscan(name, keyStart, scourStart, 2)
		if !rs.OK() {
			break
		}
		rs.KvEach(func(key, value sdb.BS) {
			fmt.Println(key.String(), sdb.B2i(value))
			keyStart = key
			scourStart = value
			ii++
		})
	}
	fmt.Println(ii)

	fmt.Println("-- Zrscan")
	db.Zrscan(name, nil, nil, 30).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), sdb.B2i(value))
	})

	fmt.Println("-- Zrscan page")
	keyStart, scourStart = nil, nil
	ii = 0
	for {
		rs := db.Zrscan(name, keyStart, scourStart, 2)
		if !rs.OK() {
			break
		}
		rs.KvEach(func(key, value sdb.BS) {
			fmt.Println(key.String(), sdb.B2i(value))
			keyStart = key
			scourStart = value
			ii++
		})
	}
	fmt.Println(ii)

	fmt.Println("hprefix")
	name = "n1"
	db.Hset(name, sdb.Bconcat([][]byte{sdb.I2b(1), sdb.I2b(1)}), nil)
	db.Hset(name, sdb.Bconcat([][]byte{sdb.I2b(1), sdb.I2b(2)}), nil)
	db.Hset(name, sdb.Bconcat([][]byte{sdb.I2b(1), sdb.I2b(3)}), nil)
	db.Hset(name, sdb.Bconcat([][]byte{sdb.I2b(1), sdb.I2b(4)}), nil)
	db.Hset(name, sdb.Bconcat([][]byte{sdb.I2b(1), sdb.I2b(5)}), nil)
	db.Hset(name, sdb.Bconcat([][]byte{sdb.I2b(1), sdb.I2b(6)}), nil)

	db.Hset(name, sdb.Bconcat([][]byte{sdb.I2b(2), sdb.I2b(1)}), nil)
	db.Hset(name, sdb.Bconcat([][]byte{sdb.I2b(2), sdb.I2b(2)}), nil)
	db.Hset(name, sdb.Bconcat([][]byte{sdb.I2b(2), sdb.I2b(3)}), nil)
	db.Hset(name, sdb.Bconcat([][]byte{sdb.I2b(2), sdb.I2b(4)}), nil)
	db.Hset(name, sdb.Bconcat([][]byte{sdb.I2b(2), sdb.I2b(5)}), nil)
	db.Hset(name, sdb.Bconcat([][]byte{sdb.I2b(2), sdb.I2b(6)}), nil)

	db.Hset("n2", sdb.Bconcat([][]byte{sdb.I2b(1), []byte("a")}), "av")
	db.Hset("n2", sdb.Bconcat([][]byte{sdb.I2b(1), []byte("b")}), "bv")
	db.Hset("n2", sdb.Bconcat([][]byte{sdb.I2b(1), []byte("c")}), "cv")
	db.Hset("n2", sdb.Bconcat([][]byte{sdb.I2b(1), []byte("d")}), "dv")

	db.Hset("n2", sdb.Bconcat([][]byte{[]byte("qq"), []byte("aq")}), "avq")
	db.Hset("n2", sdb.Bconcat([][]byte{[]byte("qq"), []byte("bq")}), "bvq")
	db.Hset("n2", sdb.Bconcat([][]byte{[]byte("qq"), []byte("qc")}), "cvq")
	db.Hset("n2", sdb.Bconcat([][]byte{[]byte("qq"), []byte("dd")}), "dvq")

	db.Hprefix(name, sdb.I2b(1), 8).KvEach(func(key, value sdb.BS) {
		fmt.Println(sdb.B2i(key[:8]))
	})

	fmt.Println("hprefix")
	db.Hprefix(name, sdb.I2b(2), 8).KvEach(func(key, value sdb.BS) {
		fmt.Println(sdb.B2i(key[:8]))
	})

	fmt.Println("hprefix")
	db.Hprefix(name, sdb.I2b(0), 8).KvEach(func(key, value sdb.BS) {
		fmt.Println(sdb.B2i(key[:8]), sdb.B2i(key[8:]))
	})

	fmt.Println("hprefix")
	db.Hprefix("n2", sdb.I2b(1), 8).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("hprefix")
	db.Hprefix("n2", []byte("qq"), 8).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("Hscan")
	db.Hscan(name, sdb.I2b(1), 8).KvEach(func(key, value sdb.BS) {
		fmt.Println(sdb.B2i(key[:8]), sdb.B2i(key[8:]))
	})

}
