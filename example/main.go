package main

import (
	"fmt"
	"github.com/ego008/sdb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"log"
	"strconv"
)

func main() {
	o := &opt.Options{
		Filter: filter.NewBloomFilter(10), // 一般取10
	}
	db, err := sdb.Open("testdb", o)
	if err != nil {
		log.Fatalln(err)
	}

	vals := [][]byte{
		[]byte("my value"),
		[]byte("byte value"),
	}

	name := "mytest"
	key := []byte("mykey")

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
	rs := db.Hget("mytest2", []byte("mykey2"))
	if !rs.NotFound() {
		log.Fatalln(rs.State)
	}
	fmt.Println(rs.State, string(rs.String()))

	// hmset + hmget
	var kvs [][]byte
	for i := 1; i < 9; i++ {
		kvs = append(kvs, []byte("k"+strconv.Itoa(i)))
	}

	err = db.Hmset(name, kvs...)
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
	fmt.Println(db.Hdel("count", []byte("mytest")))
	fmt.Println(db.Hincr("count", []byte("mytest"), 12))
	fmt.Println(db.HgetInt("count", []byte("mytest"))) // 12

	fmt.Println(db.Hmdel(name, [][]byte{[]byte("k1"), []byte("k2")}))
	fmt.Println(db.Hget(name, []byte("k1")))
	fmt.Println(db.Hset(name, []byte("k1"), []byte("v11")))
	fmt.Println(db.Hget(name, []byte("k1")))
	fmt.Println(db.HdelBucket(name))
	fmt.Println(db.Hget(name, []byte("k1")))
	fmt.Println(db.HgetInt("count", []byte("mytest"))) // 12

	fmt.Println(db.HdelBucket(name))
	_ = db.Hmset(name, kvs...)
	fmt.Println("--- Hmget ---")
	db.Hmget(name, [][]byte{[]byte("k1"), []byte("k2"), []byte("k3"), []byte("k4")}).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})
	fmt.Println("--- Hscan ---")
	db.Hscan(name, []byte("k2"), 2).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("--- Hscan 2 ---")
	db.Hscan(name, []byte("k6"), 4).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("--- Hscan 3 ---")
	db.Hscan(name, nil, 9).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("--- Hrscan ---")
	db.Hrscan(name, []byte("k5"), 2).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("--- Hrscan 2 ---")
	db.Hrscan(name, []byte("k7"), 4).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("--- Hrscan 3 ---")
	db.Hrscan(name, nil, 9).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), value.String())
	})

	fmt.Println("-- Hscan page")
	var keyStart []byte
	ii := 0
	for {
		rs := db.Hscan(name, keyStart, 2)
		if !rs.OK() {
			break
		}
		rs.KvEach(func(key, value sdb.BS) {
			fmt.Println(key, value)
			keyStart = key
			ii++
		})
	}
	fmt.Println(ii)

	fmt.Println("zet set/get")
	k := []byte("k1")
	fmt.Println(db.Zset(name, k, 12))
	fmt.Println(db.Zget(name, k))
	fmt.Println(db.Zincr(name, k, 2))
	fmt.Println(db.Zincr(name, k, -3))
	fmt.Println(db.Zdel(name, k))
	fmt.Println(db.Zget(name, k))
	fmt.Println(db.Zincr(name, k, 2))
	fmt.Println(db.Zincr(name, []byte("k2"), 2))
	fmt.Println(db.ZdelBucket(name))
	fmt.Println(db.Zincr(name, k, 2))
	fmt.Println(db.Zincr(name, []byte("k2"), 2))

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
	db.Zscan(name, []byte("k2"), sdb.I2b(2), 30).KvEach(func(key, value sdb.BS) {
		fmt.Println(key.String(), sdb.B2i(value))
	})

	fmt.Println("-- Zscan page")
	keyStart = keyStart[:0]
	var scourStart []byte
	ii = 0
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
	db.Hset(name, sdb.Bconcat(sdb.I2b(1), sdb.I2b(1)), nil)
	db.Hset(name, sdb.Bconcat(sdb.I2b(1), sdb.I2b(2)), nil)
	db.Hset(name, sdb.Bconcat(sdb.I2b(1), sdb.I2b(3)), nil)
	db.Hset(name, sdb.Bconcat(sdb.I2b(1), sdb.I2b(4)), nil)
	db.Hset(name, sdb.Bconcat(sdb.I2b(1), sdb.I2b(5)), nil)
	db.Hset(name, sdb.Bconcat(sdb.I2b(1), sdb.I2b(6)), nil)

	db.Hset(name, sdb.Bconcat(sdb.I2b(2), sdb.I2b(1)), nil)
	db.Hset(name, sdb.Bconcat(sdb.I2b(2), sdb.I2b(2)), nil)
	db.Hset(name, sdb.Bconcat(sdb.I2b(2), sdb.I2b(3)), nil)
	db.Hset(name, sdb.Bconcat(sdb.I2b(2), sdb.I2b(4)), nil)
	db.Hset(name, sdb.Bconcat(sdb.I2b(2), sdb.I2b(5)), nil)
	db.Hset(name, sdb.Bconcat(sdb.I2b(2), sdb.I2b(6)), nil)

	db.Hset("n2", sdb.Bconcat(sdb.I2b(1), []byte("a")), []byte("av"))
	db.Hset("n2", sdb.Bconcat(sdb.I2b(1), []byte("b")), []byte("bv"))
	db.Hset("n2", sdb.Bconcat(sdb.I2b(1), []byte("c")), []byte("cv"))
	db.Hset("n2", sdb.Bconcat(sdb.I2b(1), []byte("d")), []byte("dv"))

	db.Hset("n2", sdb.Bconcat([]byte("qq"), []byte("aq")), []byte("avq"))
	db.Hset("n2", sdb.Bconcat([]byte("qq"), []byte("bq")), []byte("bvq"))
	db.Hset("n2", sdb.Bconcat([]byte("qq"), []byte("qc")), []byte("cvq"))
	db.Hset("n2", sdb.Bconcat([]byte("qq"), []byte("dd")), []byte("dvq"))

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

	//
	num := uint64(1583660405608876000)
	db.Hset("aaa", sdb.I2b(num), nil)
	db.Hset("aaa", sdb.I2b(num+1), nil)
	db.Hset("aaa", sdb.I2b(num+2), nil)

	db.Hscan("aaa", sdb.I2b(num), 4).KvEach(func(key, value sdb.BS) {
		fmt.Println(sdb.B2i(key.Bytes()))
	})

}
