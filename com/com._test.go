package com_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

type TimeValue struct {
	time.Time
}

func redisOptions() *redis.Options {
	return &redis.Options{
		Addr: ":6380",
		DB:   15,

		DialTimeout:           10 * time.Second,
		ReadTimeout:           30 * time.Second,
		WriteTimeout:          30 * time.Second,
		ContextTimeoutEnabled: true,

		MaxRetries: -1,

		PoolSize:        10,
		PoolTimeout:     30 * time.Second,
		ConnMaxIdleTime: time.Minute,
	}
}

func (t *TimeValue) ScanRedis(s string) (err error) {
	t.Time, err = time.Parse(time.RFC3339Nano, s)
	return
}

var _ = Describe("Commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("server", func() {
		It("should Auth", func() {
			cmds, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Auth(ctx, "password")
				pipe.Auth(ctx, "")
				return nil
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ERR AUTH"))
			Expect(cmds[0].Err().Error()).To(ContainSubstring("ERR AUTH"))
			Expect(cmds[1].Err().Error()).To(ContainSubstring("ERR AUTH"))

			stats := client.PoolStats()
			Expect(stats.Hits).To(Equal(uint32(1)))
			Expect(stats.Misses).To(Equal(uint32(1)))
			Expect(stats.Timeouts).To(Equal(uint32(0)))
			Expect(stats.TotalConns).To(Equal(uint32(1)))
			Expect(stats.IdleConns).To(Equal(uint32(1)))
		})

		It("should hello", func() {
			cmds, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Hello(ctx, 3, "", "", "")
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			m, err := cmds[0].(*redis.MapStringInterfaceCmd).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(m["proto"]).To(Equal(int64(3)))
		})

		It("should Echo", func() {
			pipe := client.Pipeline()
			echo := pipe.Echo(ctx, "hello")
			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(echo.Err()).NotTo(HaveOccurred())
			Expect(echo.Val()).To(Equal("hello"))
		})

		It("should Ping", func() {
			ping := client.Ping(ctx)
			Expect(ping.Err()).NotTo(HaveOccurred())
			Expect(ping.Val()).To(Equal("PONG"))
		})

		It("should Wait", func() {
			const wait = 3 * time.Second

			// assume testing on single redis instance
			start := time.Now()
			val, err := client.Wait(ctx, 1, wait).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(0)))
			Expect(time.Now()).To(BeTemporally("~", start.Add(wait), 3*time.Second))
		})

		It("should Select", func() {
			pipe := client.Pipeline()
			sel := pipe.Select(ctx, 1)
			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(sel.Err()).NotTo(HaveOccurred())
			Expect(sel.Val()).To(Equal("OK"))
		})

		It("should SwapDB", func() {
			pipe := client.Pipeline()
			sel := pipe.SwapDB(ctx, 1, 2)
			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(sel.Err()).NotTo(HaveOccurred())
			Expect(sel.Val()).To(Equal("OK"))
		})

		It("should BgRewriteAOF", func() {
			Skip("flaky test")

			val, err := client.BgRewriteAOF(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(ContainSubstring("Background append only file rewriting"))
		})

		It("should BgSave", func() {
			Skip("flaky test")

			// workaround for "ERR Can't BGSAVE while AOF log rewriting is in progress"
			Eventually(func() string {
				return client.BgSave(ctx).Val()
			}, "30s").Should(Equal("Background saving started"))
		})

		It("should ClientKill", func() {
			r := client.ClientKill(ctx, "1.1.1.1:1111")
			Expect(r.Err()).To(MatchError("ERR No such client"))
			Expect(r.Val()).To(Equal(""))
		})

		It("should ClientKillByFilter", func() {
			r := client.ClientKillByFilter(ctx, "TYPE", "test")
			Expect(r.Err()).To(MatchError("ERR Unknown client type 'test'"))
			Expect(r.Val()).To(Equal(int64(0)))
		})

		It("should ClientID", func() {
			err := client.ClientID(ctx).Err()
			Expect(err).NotTo(HaveOccurred())
			Expect(client.ClientID(ctx).Val()).To(BeNumerically(">=", 0))
		})

		It("should ClientUnblock", func() {
			id := client.ClientID(ctx).Val()
			r, err := client.ClientUnblock(ctx, id).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(r).To(Equal(int64(0)))
		})

		It("should ClientUnblockWithError", func() {
			id := client.ClientID(ctx).Val()
			r, err := client.ClientUnblockWithError(ctx, id).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(r).To(Equal(int64(0)))
		})

		It("should ClientPause", func() {
			err := client.ClientPause(ctx, time.Second).Err()
			Expect(err).NotTo(HaveOccurred())

			start := time.Now()
			err = client.Ping(ctx).Err()
			Expect(err).NotTo(HaveOccurred())
			Expect(time.Now()).To(BeTemporally("~", start.Add(time.Second), 800*time.Millisecond))
		})

		It("should ClientSetName and ClientGetName", func() {
			pipe := client.Pipeline()
			set := pipe.ClientSetName(ctx, "theclientname")
			get := pipe.ClientGetName(ctx)
			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(BeTrue())

			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("theclientname"))
		})

		It("should ConfigGet", func() {
			val, err := client.ConfigGet(ctx, "*").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).NotTo(BeEmpty())
		})

		It("should ConfigResetStat", func() {
			r := client.ConfigResetStat(ctx)
			Expect(r.Err()).NotTo(HaveOccurred())
			Expect(r.Val()).To(Equal("OK"))
		})

		It("should ConfigSet", func() {
			configGet := client.ConfigGet(ctx, "maxmemory")
			Expect(configGet.Err()).NotTo(HaveOccurred())
			Expect(configGet.Val()).To(HaveLen(1))
			_, ok := configGet.Val()["maxmemory"]
			Expect(ok).To(BeTrue())

			configSet := client.ConfigSet(ctx, "maxmemory", configGet.Val()["maxmemory"])
			Expect(configSet.Err()).NotTo(HaveOccurred())
			Expect(configSet.Val()).To(Equal("OK"))
		})

		It("should ConfigRewrite", func() {
			configRewrite := client.ConfigRewrite(ctx)
			Expect(configRewrite.Err()).NotTo(HaveOccurred())
			Expect(configRewrite.Val()).To(Equal("OK"))
		})

		It("should DBSize", func() {
			size, err := client.DBSize(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(0)))
		})

		It("should Info", func() {
			info := client.Info(ctx)
			Expect(info.Err()).NotTo(HaveOccurred())
			Expect(info.Val()).NotTo(Equal(""))
		})

		It("should Info cpu", func() {
			info := client.Info(ctx, "cpu")
			Expect(info.Err()).NotTo(HaveOccurred())
			Expect(info.Val()).NotTo(Equal(""))
			Expect(info.Val()).To(ContainSubstring(`used_cpu_sys`))
		})

		It("should Info cpu and memory", func() {
			info := client.Info(ctx, "cpu", "memory")
			Expect(info.Err()).NotTo(HaveOccurred())
			Expect(info.Val()).NotTo(Equal(""))
			Expect(info.Val()).To(ContainSubstring(`used_cpu_sys`))
			Expect(info.Val()).To(ContainSubstring(`memory`))
		})

		It("should LastSave", func() {
			lastSave := client.LastSave(ctx)
			Expect(lastSave.Err()).NotTo(HaveOccurred())
			Expect(lastSave.Val()).NotTo(Equal(0))
		})

		It("should Save", func() {
			// workaround for "ERR Background save already in progress"
			Eventually(func() string {
				return client.Save(ctx).Val()
			}, "10s").Should(Equal("OK"))
		})

		It("should SlaveOf", func() {
			slaveOf := client.SlaveOf(ctx, "localhost", "8888")
			Expect(slaveOf.Err()).NotTo(HaveOccurred())
			Expect(slaveOf.Val()).To(Equal("OK"))

			slaveOf = client.SlaveOf(ctx, "NO", "ONE")
			Expect(slaveOf.Err()).NotTo(HaveOccurred())
			Expect(slaveOf.Val()).To(Equal("OK"))
		})

		It("should Time", func() {
			tm, err := client.Time(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(tm).To(BeTemporally("~", time.Now(), 3*time.Second))
		})

		It("should Command", func() {
			cmds, err := client.Command(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(cmds)).To(BeNumerically("~", 240, 25))

			cmd := cmds["mget"]
			Expect(cmd.Name).To(Equal("mget"))
			Expect(cmd.Arity).To(Equal(int8(-2)))
			Expect(cmd.Flags).To(ContainElement("readonly"))
			Expect(cmd.FirstKeyPos).To(Equal(int8(1)))
			Expect(cmd.LastKeyPos).To(Equal(int8(-1)))
			Expect(cmd.StepCount).To(Equal(int8(1)))

			cmd = cmds["ping"]
			Expect(cmd.Name).To(Equal("ping"))
			Expect(cmd.Arity).To(Equal(int8(-1)))
			Expect(cmd.Flags).To(ContainElement("fast"))
			Expect(cmd.FirstKeyPos).To(Equal(int8(0)))
			Expect(cmd.LastKeyPos).To(Equal(int8(0)))
			Expect(cmd.StepCount).To(Equal(int8(0)))
		})
	})

	Describe("debugging", func() {
		PIt("should DebugObject", func() {
			err := client.DebugObject(ctx, "foo").Err()
			Expect(err).To(MatchError("ERR no such key"))

			err = client.Set(ctx, "foo", "bar", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			s, err := client.DebugObject(ctx, "foo").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(s).To(ContainSubstring("serializedlength:4"))
		})

		It("should MemoryUsage", func() {
			err := client.MemoryUsage(ctx, "foo").Err()
			Expect(err).To(Equal(redis.Nil))

			err = client.Set(ctx, "foo", "bar", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			n, err := client.MemoryUsage(ctx, "foo").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).NotTo(BeZero())

			n, err = client.MemoryUsage(ctx, "foo", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).NotTo(BeZero())
		})
	})

	Describe("keys", func() {
		It("should Del", func() {
			err := client.Set(ctx, "key1", "Hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.Set(ctx, "key2", "World", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			n, err := client.Del(ctx, "key1", "key2", "key3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))
		})

		It("should Unlink", func() {
			err := client.Set(ctx, "key1", "Hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.Set(ctx, "key2", "World", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			n, err := client.Unlink(ctx, "key1", "key2", "key3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))
		})

		It("should Dump", func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			dump := client.Dump(ctx, "key")
			Expect(dump.Err()).NotTo(HaveOccurred())
			Expect(dump.Val()).NotTo(BeEmpty())
		})

		It("should Exists", func() {
			set := client.Set(ctx, "key1", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			n, err := client.Exists(ctx, "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))

			n, err = client.Exists(ctx, "key2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))

			n, err = client.Exists(ctx, "key1", "key2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))

			n, err = client.Exists(ctx, "key1", "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))
		})

		It("should Expire", func() {
			set := client.Set(ctx, "key", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expire := client.Expire(ctx, "key", 10*time.Second)
			Expect(expire.Err()).NotTo(HaveOccurred())
			Expect(expire.Val()).To(Equal(true))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(10 * time.Second))

			set = client.Set(ctx, "key", "Hello World", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			ttl = client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(time.Duration(-1)))

			ttl = client.TTL(ctx, "nonexistent_key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(time.Duration(-2)))
		})

		It("should ExpireAt", func() {
			setCmd := client.Set(ctx, "key", "Hello", 0)
			Expect(setCmd.Err()).NotTo(HaveOccurred())
			Expect(setCmd.Val()).To(Equal("OK"))

			n, err := client.Exists(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))

			// Check correct expiration time is set in the future
			expireAt := time.Now().Add(time.Minute)
			expireAtCmd := client.ExpireAt(ctx, "key", expireAt)
			Expect(expireAtCmd.Err()).NotTo(HaveOccurred())
			Expect(expireAtCmd.Val()).To(Equal(true))

			timeCmd := client.ExpireTime(ctx, "key")
			Expect(timeCmd.Err()).NotTo(HaveOccurred())
			Expect(timeCmd.Val().Seconds()).To(BeNumerically("==", expireAt.Unix()))

			// Check correct expiration in the past
			expireAtCmd = client.ExpireAt(ctx, "key", time.Now().Add(-time.Hour))
			Expect(expireAtCmd.Err()).NotTo(HaveOccurred())
			Expect(expireAtCmd.Val()).To(Equal(true))

			n, err = client.Exists(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))

		})

		It("should Keys", func() {
			mset := client.MSet(ctx, "one", "1", "two", "2", "three", "3", "four", "4")
			Expect(mset.Err()).NotTo(HaveOccurred())
			Expect(mset.Val()).To(Equal("OK"))

			keys := client.Keys(ctx, "*o*")
			Expect(keys.Err()).NotTo(HaveOccurred())
			Expect(keys.Val()).To(ConsistOf([]string{"four", "one", "two"}))

			keys = client.Keys(ctx, "t??")
			Expect(keys.Err()).NotTo(HaveOccurred())
			Expect(keys.Val()).To(Equal([]string{"two"}))

			keys = client.Keys(ctx, "*")
			Expect(keys.Err()).NotTo(HaveOccurred())
			Expect(keys.Val()).To(ConsistOf([]string{"four", "one", "three", "two"}))
		})

		It("should Migrate", func() {
			migrate := client.Migrate(ctx, "localhost", "6381", "key", 0, 0)
			Expect(migrate.Err()).NotTo(HaveOccurred())
			Expect(migrate.Val()).To(Equal("NOKEY"))

			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			migrate = client.Migrate(ctx, "localhost", "6381", "key", 0, 0)
			Expect(migrate.Err()).To(MatchError("IOERR error or timeout writing to target instance"))
			Expect(migrate.Val()).To(Equal(""))
		})

		It("should Move", func() {
			move := client.Move(ctx, "key", 2)
			Expect(move.Err()).NotTo(HaveOccurred())
			Expect(move.Val()).To(Equal(false))

			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			move = client.Move(ctx, "key", 2)
			Expect(move.Err()).NotTo(HaveOccurred())
			Expect(move.Val()).To(Equal(true))

			get := client.Get(ctx, "key")
			Expect(get.Err()).To(Equal(redis.Nil))
			Expect(get.Val()).To(Equal(""))

			pipe := client.Pipeline()
			pipe.Select(ctx, 2)
			get = pipe.Get(ctx, "key")
			pipe.FlushDB(ctx)

			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should Object", func() {
			start := time.Now()
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			refCount := client.ObjectRefCount(ctx, "key")
			Expect(refCount.Err()).NotTo(HaveOccurred())
			Expect(refCount.Val()).To(Equal(int64(1)))

			err := client.ObjectEncoding(ctx, "key").Err()
			Expect(err).NotTo(HaveOccurred())

			idleTime := client.ObjectIdleTime(ctx, "key")
			Expect(idleTime.Err()).NotTo(HaveOccurred())

			// Redis returned milliseconds/1000, which may cause ObjectIdleTime to be at a critical value,
			// should be +1s to deal with the critical value problem.
			// if too much time (>1s) is used during command execution, it may also cause the test to fail.
			// so the ObjectIdleTime result should be <=now-start+1s
			// link: https://github.com/redis/redis/blob/5b48d900498c85bbf4772c1d466c214439888115/src/object.c#L1265-L1272
			Expect(idleTime.Val()).To(BeNumerically("<=", time.Now().Sub(start)+time.Second))
		})

		It("should Persist", func() {
			set := client.Set(ctx, "key", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expire := client.Expire(ctx, "key", 10*time.Second)
			Expect(expire.Err()).NotTo(HaveOccurred())
			Expect(expire.Val()).To(Equal(true))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(10 * time.Second))

			persist := client.Persist(ctx, "key")
			Expect(persist.Err()).NotTo(HaveOccurred())
			Expect(persist.Val()).To(Equal(true))

			ttl = client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val() < 0).To(Equal(true))
		})

		It("should PExpire", func() {
			set := client.Set(ctx, "key", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expiration := 900 * time.Millisecond
			pexpire := client.PExpire(ctx, "key", expiration)
			Expect(pexpire.Err()).NotTo(HaveOccurred())
			Expect(pexpire.Val()).To(Equal(true))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(time.Second))

			pttl := client.PTTL(ctx, "key")
			Expect(pttl.Err()).NotTo(HaveOccurred())
			Expect(pttl.Val()).To(BeNumerically("~", expiration, 100*time.Millisecond))
		})

		It("should PExpireAt", func() {
			set := client.Set(ctx, "key", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expiration := 900 * time.Millisecond
			pexpireat := client.PExpireAt(ctx, "key", time.Now().Add(expiration))
			Expect(pexpireat.Err()).NotTo(HaveOccurred())
			Expect(pexpireat.Val()).To(Equal(true))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(time.Second))

			pttl := client.PTTL(ctx, "key")
			Expect(pttl.Err()).NotTo(HaveOccurred())
			Expect(pttl.Val()).To(BeNumerically("~", expiration, 100*time.Millisecond))
		})

		It("should PExpireTime", func() {

			// The command returns -1 if the key exists but has no associated expiration time.
			// The command returns -2 if the key does not exist.
			pExpireTime := client.PExpireTime(ctx, "key")
			Expect(pExpireTime.Err()).NotTo(HaveOccurred())
			Expect(pExpireTime.Val() < 0).To(Equal(true))

			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			timestamp := time.Now().Add(time.Minute)
			expireAt := client.PExpireAt(ctx, "key", timestamp)
			Expect(expireAt.Err()).NotTo(HaveOccurred())
			Expect(expireAt.Val()).To(Equal(true))

			pExpireTime = client.PExpireTime(ctx, "key")
			Expect(pExpireTime.Err()).NotTo(HaveOccurred())
			Expect(pExpireTime.Val().Milliseconds()).To(BeNumerically("==", timestamp.UnixMilli()))
		})

		It("should PTTL", func() {
			set := client.Set(ctx, "key", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expiration := time.Second
			expire := client.Expire(ctx, "key", expiration)
			Expect(expire.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			pttl := client.PTTL(ctx, "key")
			Expect(pttl.Err()).NotTo(HaveOccurred())
			Expect(pttl.Val()).To(BeNumerically("~", expiration, 100*time.Millisecond))
		})

		It("should RandomKey", func() {
			randomKey := client.RandomKey(ctx)
			Expect(randomKey.Err()).To(Equal(redis.Nil))
			Expect(randomKey.Val()).To(Equal(""))

			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			randomKey = client.RandomKey(ctx)
			Expect(randomKey.Err()).NotTo(HaveOccurred())
			Expect(randomKey.Val()).To(Equal("key"))
		})

		It("should Rename", func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			status := client.Rename(ctx, "key", "key1")
			Expect(status.Err()).NotTo(HaveOccurred())
			Expect(status.Val()).To(Equal("OK"))

			get := client.Get(ctx, "key1")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should RenameNX", func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			renameNX := client.RenameNX(ctx, "key", "key1")
			Expect(renameNX.Err()).NotTo(HaveOccurred())
			Expect(renameNX.Val()).To(Equal(true))

			get := client.Get(ctx, "key1")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should Restore", func() {
			err := client.Set(ctx, "key", "hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			dump := client.Dump(ctx, "key")
			Expect(dump.Err()).NotTo(HaveOccurred())

			err = client.Del(ctx, "key").Err()
			Expect(err).NotTo(HaveOccurred())

			restore, err := client.Restore(ctx, "key", 0, dump.Val()).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(restore).To(Equal("OK"))

			type_, err := client.Type(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(type_).To(Equal("string"))

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))
		})

		It("should RestoreReplace", func() {
			err := client.Set(ctx, "key", "hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			dump := client.Dump(ctx, "key")
			Expect(dump.Err()).NotTo(HaveOccurred())

			restore, err := client.RestoreReplace(ctx, "key", 0, dump.Val()).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(restore).To(Equal("OK"))

			type_, err := client.Type(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(type_).To(Equal("string"))

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))
		})

		It("should Sort RO", func() {
			size, err := client.LPush(ctx, "list", "1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(1)))

			size, err = client.LPush(ctx, "list", "3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(2)))

			size, err = client.LPush(ctx, "list", "2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(3)))

			els, err := client.SortRO(ctx, "list", &redis.Sort{
				Offset: 0,
				Count:  2,
				Order:  "ASC",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(els).To(Equal([]string{"1", "2"}))
		})

		It("should Sort", func() {
			size, err := client.LPush(ctx, "list", "1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(1)))

			size, err = client.LPush(ctx, "list", "3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(2)))

			size, err = client.LPush(ctx, "list", "2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(3)))

			els, err := client.Sort(ctx, "list", &redis.Sort{
				Offset: 0,
				Count:  2,
				Order:  "ASC",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(els).To(Equal([]string{"1", "2"}))
		})

		It("should Sort and Get", func() {
			size, err := client.LPush(ctx, "list", "1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(1)))

			size, err = client.LPush(ctx, "list", "3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(2)))

			size, err = client.LPush(ctx, "list", "2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(3)))

			err = client.Set(ctx, "object_2", "value2", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			{
				els, err := client.Sort(ctx, "list", &redis.Sort{
					Get: []string{"object_*"},
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(els).To(Equal([]string{"", "value2", ""}))
			}

			{
				els, err := client.SortInterfaces(ctx, "list", &redis.Sort{
					Get: []string{"object_*"},
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(els).To(Equal([]interface{}{nil, "value2", nil}))
			}
		})

		It("should Sort and Store", func() {
			size, err := client.LPush(ctx, "list", "1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(1)))

			size, err = client.LPush(ctx, "list", "3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(2)))

			size, err = client.LPush(ctx, "list", "2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(3)))

			n, err := client.SortStore(ctx, "list", "list2", &redis.Sort{
				Offset: 0,
				Count:  2,
				Order:  "ASC",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))

			els, err := client.LRange(ctx, "list2", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(els).To(Equal([]string{"1", "2"}))
		})

		It("should Touch", func() {
			set1 := client.Set(ctx, "touch1", "hello", 0)
			Expect(set1.Err()).NotTo(HaveOccurred())
			Expect(set1.Val()).To(Equal("OK"))

			set2 := client.Set(ctx, "touch2", "hello", 0)
			Expect(set2.Err()).NotTo(HaveOccurred())
			Expect(set2.Val()).To(Equal("OK"))

			touch := client.Touch(ctx, "touch1", "touch2", "touch3")
			Expect(touch.Err()).NotTo(HaveOccurred())
			Expect(touch.Val()).To(Equal(int64(2)))
		})

		It("should ExpireTime", func() {

			// The command returns -1 if the key exists but has no associated expiration time.
			// The command returns -2 if the key does not exist.
			expireTimeCmd := client.ExpireTime(ctx, "key")
			Expect(expireTimeCmd.Err()).NotTo(HaveOccurred())
			Expect(expireTimeCmd.Val() < 0).To(Equal(true))

			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expireAt := time.Now().Add(time.Minute)
			expireAtCmd := client.ExpireAt(ctx, "key", expireAt)
			Expect(expireAtCmd.Err()).NotTo(HaveOccurred())
			Expect(expireAtCmd.Val()).To(Equal(true))

			expireTimeCmd = client.ExpireTime(ctx, "key")
			Expect(expireTimeCmd.Err()).NotTo(HaveOccurred())
			Expect(expireTimeCmd.Val().Seconds()).To(BeNumerically("==", expireAt.Unix()))
		})

		It("should TTL", func() {

			// The command returns -1 if the key exists but has no associated expire
			// The command returns -2 if the key does not exist.
			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val() < 0).To(Equal(true))

			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expire := client.Expire(ctx, "key", 60*time.Second)
			Expect(expire.Err()).NotTo(HaveOccurred())
			Expect(expire.Val()).To(Equal(true))

			ttl = client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(60 * time.Second))
		})

		It("should Type", func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			type_ := client.Type(ctx, "key")
			Expect(type_.Err()).NotTo(HaveOccurred())
			Expect(type_.Val()).To(Equal("string"))
		})
	})

	Describe("scanning", func() {
		It("should Scan", func() {
			for i := 0; i < 1000; i++ {
				set := client.Set(ctx, fmt.Sprintf("key%d", i), "hello", 0)
				Expect(set.Err()).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.Scan(ctx, 0, "", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).NotTo(BeEmpty())
			Expect(cursor).NotTo(BeZero())
		})

		It("should ScanType", func() {
			for i := 0; i < 1000; i++ {
				set := client.Set(ctx, fmt.Sprintf("key%d", i), "hello", 0)
				Expect(set.Err()).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.ScanType(ctx, 0, "", 0, "string").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).NotTo(BeEmpty())
			Expect(cursor).NotTo(BeZero())
		})

		It("should SScan", func() {
			for i := 0; i < 1000; i++ {
				sadd := client.SAdd(ctx, "myset", fmt.Sprintf("member%d", i))
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.SScan(ctx, "myset", 0, "", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).NotTo(BeEmpty())
			Expect(cursor).NotTo(BeZero())
		})

		It("should HScan", func() {
			for i := 0; i < 1000; i++ {
				sadd := client.HSet(ctx, "myhash", fmt.Sprintf("key%d", i), "hello")
				Expect(sadd.Err()).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.HScan(ctx, "myhash", 0, "", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).NotTo(BeEmpty())
			Expect(cursor).NotTo(BeZero())
		})

		It("should ZScan", func() {
			for i := 0; i < 1000; i++ {
				err := client.ZAdd(ctx, "myset", redis.Z{
					Score:  float64(i),
					Member: fmt.Sprintf("member%d", i),
				}).Err()
				Expect(err).NotTo(HaveOccurred())
			}

			keys, cursor, err := client.ZScan(ctx, "myset", 0, "", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).NotTo(BeEmpty())
			Expect(cursor).NotTo(BeZero())
		})
	})

	Describe("strings", func() {
		It("should Append", func() {
			n, err := client.Exists(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))

			appendRes := client.Append(ctx, "key", "Hello")
			Expect(appendRes.Err()).NotTo(HaveOccurred())
			Expect(appendRes.Val()).To(Equal(int64(5)))

			appendRes = client.Append(ctx, "key", " World")
			Expect(appendRes.Err()).NotTo(HaveOccurred())
			Expect(appendRes.Val()).To(Equal(int64(11)))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("Hello World"))
		})

		It("should BitCount", func() {
			set := client.Set(ctx, "key", "foobar", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitCount := client.BitCount(ctx, "key", nil)
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(26)))

			bitCount = client.BitCount(ctx, "key", &redis.BitCount{
				Start: 0,
				End:   0,
			})
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(4)))

			bitCount = client.BitCount(ctx, "key", &redis.BitCount{
				Start: 1,
				End:   1,
			})
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(6)))
		})

		It("should BitOpAnd", func() {
			set := client.Set(ctx, "key1", "1", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			set = client.Set(ctx, "key2", "0", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpAnd := client.BitOpAnd(ctx, "dest", "key1", "key2")
			Expect(bitOpAnd.Err()).NotTo(HaveOccurred())
			Expect(bitOpAnd.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("0"))
		})

		It("should BitOpOr", func() {
			set := client.Set(ctx, "key1", "1", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			set = client.Set(ctx, "key2", "0", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpOr := client.BitOpOr(ctx, "dest", "key1", "key2")
			Expect(bitOpOr.Err()).NotTo(HaveOccurred())
			Expect(bitOpOr.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("1"))
		})

		It("should BitOpXor", func() {
			set := client.Set(ctx, "key1", "\xff", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			set = client.Set(ctx, "key2", "\x0f", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpXor := client.BitOpXor(ctx, "dest", "key1", "key2")
			Expect(bitOpXor.Err()).NotTo(HaveOccurred())
			Expect(bitOpXor.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("\xf0"))
		})

		It("should BitOpNot", func() {
			set := client.Set(ctx, "key1", "\x00", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitOpNot := client.BitOpNot(ctx, "dest", "key1")
			Expect(bitOpNot.Err()).NotTo(HaveOccurred())
			Expect(bitOpNot.Val()).To(Equal(int64(1)))

			get := client.Get(ctx, "dest")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("\xff"))
		})

		It("should BitPos", func() {
			err := client.Set(ctx, "mykey", "\xff\xf0\x00", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			pos, err := client.BitPos(ctx, "mykey", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(12)))

			pos, err = client.BitPos(ctx, "mykey", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(0)))

			pos, err = client.BitPos(ctx, "mykey", 0, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(16)))

			pos, err = client.BitPos(ctx, "mykey", 1, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, "mykey", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(16)))

			pos, err = client.BitPos(ctx, "mykey", 1, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, "mykey", 0, 2, 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, "mykey", 0, 0, -3).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))

			pos, err = client.BitPos(ctx, "mykey", 0, 0, 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(Equal(int64(-1)))
		})

		It("should BitField", func() {
			nn, err := client.BitField(ctx, "mykey", "INCRBY", "i5", 100, 1, "GET", "u4", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(nn).To(Equal([]int64{1, 0}))
		})

		It("should Decr", func() {
			set := client.Set(ctx, "key", "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			decr := client.Decr(ctx, "key")
			Expect(decr.Err()).NotTo(HaveOccurred())
			Expect(decr.Val()).To(Equal(int64(9)))

			set = client.Set(ctx, "key", "234293482390480948029348230948", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			decr = client.Decr(ctx, "key")
			Expect(decr.Err()).To(MatchError("ERR value is not an integer or out of range"))
			Expect(decr.Val()).To(Equal(int64(0)))
		})

		It("should DecrBy", func() {
			set := client.Set(ctx, "key", "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			decrBy := client.DecrBy(ctx, "key", 5)
			Expect(decrBy.Err()).NotTo(HaveOccurred())
			Expect(decrBy.Val()).To(Equal(int64(5)))
		})

		It("should Get", func() {
			get := client.Get(ctx, "_")
			Expect(get.Err()).To(Equal(redis.Nil))
			Expect(get.Val()).To(Equal(""))

			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			get = client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should GetBit", func() {
			setBit := client.SetBit(ctx, "key", 7, 1)
			Expect(setBit.Err()).NotTo(HaveOccurred())
			Expect(setBit.Val()).To(Equal(int64(0)))

			getBit := client.GetBit(ctx, "key", 0)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(0)))

			getBit = client.GetBit(ctx, "key", 7)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(1)))

			getBit = client.GetBit(ctx, "key", 100)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(0)))
		})

		It("should GetRange", func() {
			set := client.Set(ctx, "key", "This is a string", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			getRange := client.GetRange(ctx, "key", 0, 3)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("This"))

			getRange = client.GetRange(ctx, "key", -3, -1)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("ing"))

			getRange = client.GetRange(ctx, "key", 0, -1)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("This is a string"))

			getRange = client.GetRange(ctx, "key", 10, 100)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("string"))
		})

		It("should GetSet", func() {
			incr := client.Incr(ctx, "key")
			Expect(incr.Err()).NotTo(HaveOccurred())
			Expect(incr.Val()).To(Equal(int64(1)))

			getSet := client.GetSet(ctx, "key", "0")
			Expect(getSet.Err()).NotTo(HaveOccurred())
			Expect(getSet.Val()).To(Equal("1"))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("0"))
		})

		It("should GetEX", func() {
			set := client.Set(ctx, "key", "value", 100*time.Second)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(BeNumerically("~", 100*time.Second, 3*time.Second))

			getEX := client.GetEx(ctx, "key", 200*time.Second)
			Expect(getEX.Err()).NotTo(HaveOccurred())
			Expect(getEX.Val()).To(Equal("value"))

			ttl = client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(BeNumerically("~", 200*time.Second, 3*time.Second))
		})

		It("should GetDel", func() {
			set := client.Set(ctx, "key", "value", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			getDel := client.GetDel(ctx, "key")
			Expect(getDel.Err()).NotTo(HaveOccurred())
			Expect(getDel.Val()).To(Equal("value"))

			get := client.Get(ctx, "key")
			Expect(get.Err()).To(Equal(redis.Nil))
		})

		It("should Incr", func() {
			set := client.Set(ctx, "key", "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incr := client.Incr(ctx, "key")
			Expect(incr.Err()).NotTo(HaveOccurred())
			Expect(incr.Val()).To(Equal(int64(11)))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("11"))
		})

		It("should IncrBy", func() {
			set := client.Set(ctx, "key", "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incrBy := client.IncrBy(ctx, "key", 5)
			Expect(incrBy.Err()).NotTo(HaveOccurred())
			Expect(incrBy.Val()).To(Equal(int64(15)))
		})

		It("should IncrByFloat", func() {
			set := client.Set(ctx, "key", "10.50", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incrByFloat := client.IncrByFloat(ctx, "key", 0.1)
			Expect(incrByFloat.Err()).NotTo(HaveOccurred())
			Expect(incrByFloat.Val()).To(Equal(10.6))

			set = client.Set(ctx, "key", "5.0e3", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incrByFloat = client.IncrByFloat(ctx, "key", 2.0e2)
			Expect(incrByFloat.Err()).NotTo(HaveOccurred())
			Expect(incrByFloat.Val()).To(Equal(float64(5200)))
		})

		It("should IncrByFloatOverflow", func() {
			incrByFloat := client.IncrByFloat(ctx, "key", 996945661)
			Expect(incrByFloat.Err()).NotTo(HaveOccurred())
			Expect(incrByFloat.Val()).To(Equal(float64(996945661)))
		})

		It("should MSetMGet", func() {
			mSet := client.MSet(ctx, "key1", "hello1", "key2", "hello2")
			Expect(mSet.Err()).NotTo(HaveOccurred())
			Expect(mSet.Val()).To(Equal("OK"))

			mGet := client.MGet(ctx, "key1", "key2", "_")
			Expect(mGet.Err()).NotTo(HaveOccurred())
			Expect(mGet.Val()).To(Equal([]interface{}{"hello1", "hello2", nil}))

			// MSet struct
			type set struct {
				Set1 string                 `redis:"set1"`
				Set2 int16                  `redis:"set2"`
				Set3 time.Duration          `redis:"set3"`
				Set4 interface{}            `redis:"set4"`
				Set5 map[string]interface{} `redis:"-"`
			}
			mSet = client.MSet(ctx, &set{
				Set1: "val1",
				Set2: 1024,
				Set3: 2 * time.Millisecond,
				Set4: nil,
				Set5: map[string]interface{}{"k1": 1},
			})
			Expect(mSet.Err()).NotTo(HaveOccurred())
			Expect(mSet.Val()).To(Equal("OK"))

			mGet = client.MGet(ctx, "set1", "set2", "set3", "set4")
			Expect(mGet.Err()).NotTo(HaveOccurred())
			Expect(mGet.Val()).To(Equal([]interface{}{
				"val1",
				"1024",
				strconv.Itoa(int(2 * time.Millisecond.Nanoseconds())),
				"",
			}))
		})

		It("should scan Mget", func() {
			now := time.Now()

			err := client.MSet(ctx, "key1", "hello1", "key2", 123, "time", now.Format(time.RFC3339Nano)).Err()
			Expect(err).NotTo(HaveOccurred())

			res := client.MGet(ctx, "key1", "key2", "_", "time")
			Expect(res.Err()).NotTo(HaveOccurred())

			type data struct {
				Key1 string    `redis:"key1"`
				Key2 int       `redis:"key2"`
				Time TimeValue `redis:"time"`
			}
			var d data
			Expect(res.Scan(&d)).NotTo(HaveOccurred())
			Expect(d.Time.UnixNano()).To(Equal(now.UnixNano()))
			d.Time.Time = time.Time{}
			Expect(d).To(Equal(data{
				Key1: "hello1",
				Key2: 123,
				Time: TimeValue{Time: time.Time{}},
			}))
		})

		It("should MSetNX", func() {
			mSetNX := client.MSetNX(ctx, "key1", "hello1", "key2", "hello2")
			Expect(mSetNX.Err()).NotTo(HaveOccurred())
			Expect(mSetNX.Val()).To(Equal(true))

			mSetNX = client.MSetNX(ctx, "key2", "hello1", "key3", "hello2")
			Expect(mSetNX.Err()).NotTo(HaveOccurred())
			Expect(mSetNX.Val()).To(Equal(false))

			// set struct
			// MSet struct
			type set struct {
				Set1 string                 `redis:"set1"`
				Set2 int16                  `redis:"set2"`
				Set3 time.Duration          `redis:"set3"`
				Set4 interface{}            `redis:"set4"`
				Set5 map[string]interface{} `redis:"-"`
			}
			mSetNX = client.MSetNX(ctx, &set{
				Set1: "val1",
				Set2: 1024,
				Set3: 2 * time.Millisecond,
				Set4: nil,
				Set5: map[string]interface{}{"k1": 1},
			})
			Expect(mSetNX.Err()).NotTo(HaveOccurred())
			Expect(mSetNX.Val()).To(Equal(true))
		})

		It("should SetWithArgs with TTL", func() {
			args := redis.SetArgs{
				TTL: 500 * time.Millisecond,
			}
			err := client.SetArgs(ctx, "key", "hello", args).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))

			Eventually(func() error {
				return client.Get(ctx, "key").Err()
			}, "2s", "100ms").Should(Equal(redis.Nil))
		})

		It("should SetWithArgs with expiration date", func() {
			expireAt := time.Now().AddDate(1, 1, 1)
			args := redis.SetArgs{
				ExpireAt: expireAt,
			}
			err := client.SetArgs(ctx, "key", "hello", args).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))

			// check the key has an expiration date
			// (so a TTL value different of -1)
			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).ToNot(Equal(-1))
		})

		It("should SetWithArgs with negative expiration date", func() {
			args := redis.SetArgs{
				ExpireAt: time.Now().AddDate(-3, 1, 1),
			}
			// redis accepts a timestamp less than the current date
			// but returns nil when trying to get the key
			err := client.SetArgs(ctx, "key", "hello", args).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.Get(ctx, "key").Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with keepttl", func() {
			// Set with ttl
			argsWithTTL := redis.SetArgs{
				TTL: 5 * time.Second,
			}
			set := client.SetArgs(ctx, "key", "hello", argsWithTTL)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Result()).To(Equal("OK"))

			// Set with keepttl
			argsWithKeepTTL := redis.SetArgs{
				KeepTTL: true,
			}
			set = client.SetArgs(ctx, "key", "hello", argsWithKeepTTL)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Result()).To(Equal("OK"))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			// set keepttl will Retain the ttl associated with the key
			Expect(ttl.Val().Nanoseconds()).NotTo(Equal(-1))
		})

		It("should SetWithArgs with NX mode and key exists", func() {
			err := client.Set(ctx, "key", "hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			args := redis.SetArgs{
				Mode: "nx",
			}
			val, err := client.SetArgs(ctx, "key", "hello", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with NX mode and key does not exist", func() {
			args := redis.SetArgs{
				Mode: "nx",
			}
			val, err := client.SetArgs(ctx, "key", "hello", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("OK"))
		})

		It("should SetWithArgs with NX mode and GET option", func() {
			args := redis.SetArgs{
				Mode: "nx",
				Get:  true,
			}
			val, err := client.SetArgs(ctx, "key", "hello", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with expiration, NX mode, and key does not exist", func() {
			args := redis.SetArgs{
				TTL:  500 * time.Millisecond,
				Mode: "nx",
			}
			val, err := client.SetArgs(ctx, "key", "hello", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("OK"))

			Eventually(func() error {
				return client.Get(ctx, "key").Err()
			}, "1s", "100ms").Should(Equal(redis.Nil))
		})

		It("should SetWithArgs with expiration, NX mode, and key exists", func() {
			e := client.Set(ctx, "key", "hello", 0)
			Expect(e.Err()).NotTo(HaveOccurred())

			args := redis.SetArgs{
				TTL:  500 * time.Millisecond,
				Mode: "nx",
			}
			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with expiration, NX mode, and GET option", func() {
			args := redis.SetArgs{
				TTL:  500 * time.Millisecond,
				Mode: "nx",
				Get:  true,
			}
			val, err := client.SetArgs(ctx, "key", "hello", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with XX mode and key does not exist", func() {
			args := redis.SetArgs{
				Mode: "xx",
			}
			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with XX mode and key exists", func() {
			e := client.Set(ctx, "key", "hello", 0).Err()
			Expect(e).NotTo(HaveOccurred())

			args := redis.SetArgs{
				Mode: "xx",
			}
			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("OK"))
		})

		It("should SetWithArgs with XX mode and GET option, and key exists", func() {
			e := client.Set(ctx, "key", "hello", 0).Err()
			Expect(e).NotTo(HaveOccurred())

			args := redis.SetArgs{
				Mode: "xx",
				Get:  true,
			}
			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))
		})

		It("should SetWithArgs with XX mode and GET option, and key does not exist", func() {
			args := redis.SetArgs{
				Mode: "xx",
				Get:  true,
			}

			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with expiration, XX mode, GET option, and key does not exist", func() {
			args := redis.SetArgs{
				TTL:  500 * time.Millisecond,
				Mode: "xx",
				Get:  true,
			}

			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with expiration, XX mode, GET option, and key exists", func() {
			e := client.Set(ctx, "key", "hello", 0)
			Expect(e.Err()).NotTo(HaveOccurred())

			args := redis.SetArgs{
				TTL:  500 * time.Millisecond,
				Mode: "xx",
				Get:  true,
			}

			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))

			Eventually(func() error {
				return client.Get(ctx, "key").Err()
			}, "1s", "100ms").Should(Equal(redis.Nil))
		})

		It("should SetWithArgs with Get and key does not exist yet", func() {
			args := redis.SetArgs{
				Get: true,
			}

			val, err := client.SetArgs(ctx, "key", "hello", args).Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should SetWithArgs with Get and key exists", func() {
			e := client.Set(ctx, "key", "hello", 0)
			Expect(e.Err()).NotTo(HaveOccurred())

			args := redis.SetArgs{
				Get: true,
			}

			val, err := client.SetArgs(ctx, "key", "world", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))
		})

		It("should Pipelined SetArgs with Get and key exists", func() {
			e := client.Set(ctx, "key", "hello", 0)
			Expect(e.Err()).NotTo(HaveOccurred())

			args := redis.SetArgs{
				Get: true,
			}

			pipe := client.Pipeline()
			setArgs := pipe.SetArgs(ctx, "key", "world", args)
			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(setArgs.Err()).NotTo(HaveOccurred())
			Expect(setArgs.Val()).To(Equal("hello"))
		})

		It("should Set with expiration", func() {
			err := client.Set(ctx, "key", "hello", 100*time.Millisecond).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))

			Eventually(func() error {
				return client.Get(ctx, "key").Err()
			}, "1s", "100ms").Should(Equal(redis.Nil))
		})

		It("should Set with keepttl", func() {
			// set with ttl
			set := client.Set(ctx, "key", "hello", 5*time.Second)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			// set with keepttl
			set = client.Set(ctx, "key", "hello1", redis.KeepTTL)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			// set keepttl will Retain the ttl associated with the key
			Expect(ttl.Val().Nanoseconds()).NotTo(Equal(-1))
		})

		It("should SetGet", func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should SetEX", func() {
			err := client.SetEx(ctx, "key", "hello", 1*time.Second).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))

			Eventually(func() error {
				return client.Get(ctx, "foo").Err()
			}, "2s", "100ms").Should(Equal(redis.Nil))
		})

		It("should SetNX", func() {
			setNX := client.SetNX(ctx, "key", "hello", 0)
			Expect(setNX.Err()).NotTo(HaveOccurred())
			Expect(setNX.Val()).To(Equal(true))

			setNX = client.SetNX(ctx, "key", "hello2", 0)
			Expect(setNX.Err()).NotTo(HaveOccurred())
			Expect(setNX.Val()).To(Equal(false))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should SetNX with expiration", func() {
			isSet, err := client.SetNX(ctx, "key", "hello", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			isSet, err = client.SetNX(ctx, "key", "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))
		})

		It("should SetNX with keepttl", func() {
			isSet, err := client.SetNX(ctx, "key", "hello1", redis.KeepTTL).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			ttl := client.TTL(ctx, "key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val().Nanoseconds()).To(Equal(int64(-1)))
		})

		It("should SetXX", func() {
			isSet, err := client.SetXX(ctx, "key", "hello2", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			err = client.Set(ctx, "key", "hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			isSet, err = client.SetXX(ctx, "key", "hello2", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello2"))
		})

		It("should SetXX with expiration", func() {
			isSet, err := client.SetXX(ctx, "key", "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			err = client.Set(ctx, "key", "hello", time.Second).Err()
			Expect(err).NotTo(HaveOccurred())

			isSet, err = client.SetXX(ctx, "key", "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello2"))
		})

		It("should SetXX with keepttl", func() {
			isSet, err := client.SetXX(ctx, "key", "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			err = client.Set(ctx, "key", "hello", time.Second).Err()
			Expect(err).NotTo(HaveOccurred())

			isSet, err = client.SetXX(ctx, "key", "hello2", 5*time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			isSet, err = client.SetXX(ctx, "key", "hello3", redis.KeepTTL).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			val, err := client.Get(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello3"))

			// set keepttl will Retain the ttl associated with the key
			ttl, err := client.TTL(ctx, "key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ttl).NotTo(Equal(-1))
		})

		It("should SetRange", func() {
			set := client.Set(ctx, "key", "Hello World", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			range_ := client.SetRange(ctx, "key", 6, "Redis")
			Expect(range_.Err()).NotTo(HaveOccurred())
			Expect(range_.Val()).To(Equal(int64(11)))

			get := client.Get(ctx, "key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("Hello Redis"))
		})

		It("should StrLen", func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			strLen := client.StrLen(ctx, "key")
			Expect(strLen.Err()).NotTo(HaveOccurred())
			Expect(strLen.Val()).To(Equal(int64(5)))

			strLen = client.StrLen(ctx, "_")
			Expect(strLen.Err()).NotTo(HaveOccurred())
			Expect(strLen.Val()).To(Equal(int64(0)))
		})

		It("should Copy", func() {
			set := client.Set(ctx, "key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			copy := client.Copy(ctx, "key", "newKey", redisOptions().DB, false)
			Expect(copy.Err()).NotTo(HaveOccurred())
			Expect(copy.Val()).To(Equal(int64(1)))

			// Value is available by both keys now
			getOld := client.Get(ctx, "key")
			Expect(getOld.Err()).NotTo(HaveOccurred())
			Expect(getOld.Val()).To(Equal("hello"))
			getNew := client.Get(ctx, "newKey")
			Expect(getNew.Err()).NotTo(HaveOccurred())
			Expect(getNew.Val()).To(Equal("hello"))

			// Overwriting an existing key should not succeed
			overwrite := client.Copy(ctx, "newKey", "key", redisOptions().DB, false)
			Expect(overwrite.Val()).To(Equal(int64(0)))

			// Overwrite is allowed when replace=rue
			replace := client.Copy(ctx, "newKey", "key", redisOptions().DB, true)
			Expect(replace.Val()).To(Equal(int64(1)))
		})
		It("should FunctionLoad and FunctionDelete", func() {
			val, err := client.FunctionLoad(ctx, "#!lua name=mylib \n redis.register_function('myfunc', function(keys, args) return args[1] end)").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("mylib"))

			val, err = client.FunctionLoad(ctx, "#!lua name=mylib \n redis.register_function('myfunc', function(keys, args) return args[1] end)").Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ERR Library"))

			val, err = client.FunctionLoad(ctx, "#!lua name=mylib1 \n redis.register_function('myfunc', function(keys, args) return args[1] end)").Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("already exists"))

			// change the function content
			val, err = client.FunctionLoadReplace(ctx, "#!lua name=mylib \n redis.register_function('myfunc', function(keys, args) return args[2] end)").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("mylib"))

			val, err = client.FunctionLoadReplace(ctx, "#!lua name=mylib2 \n redis.register_function('myfunc2', function(keys, args) return args[1] end)").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("mylib2"))
			val, err = client.FunctionLoadReplace(ctx, "#!lua name=mylib2 \n redis.register_function('myfunc', function(keys, args) return args[1] end)").Result()
			Expect(err).To(HaveOccurred())

			val, err = client.FunctionDelete(ctx, "mylib").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("OK"))
			val, err = client.FunctionDelete(ctx, "mylib2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("OK"))

			val, err = client.FunctionDelete(ctx, "mylib").Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ERR Library not found"))

		})
	})

	Describe("SlowLogGet", func() {
		It("returns slow query result", func() {
			const key = "slowlog-log-slower-than"

			old := client.ConfigGet(ctx, key).Val()
			client.ConfigSet(ctx, key, "0")
			defer client.ConfigSet(ctx, key, old[key])

			err := client.Do(ctx, "slowlog", "reset").Err()
			Expect(err).NotTo(HaveOccurred())

			client.Set(ctx, "test", "true", 0)

			result, err := client.SlowLogGet(ctx, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).NotTo(BeZero())
		})
	})
})

type numberStruct struct {
	Number int
}

func (s *numberStruct) MarshalBinary() ([]byte, error) {
	return json.Marshal(s)
}

func (s *numberStruct) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, s)
}

func deref(viface interface{}) interface{} {
	v := reflect.ValueOf(viface)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v.Interface()
}
