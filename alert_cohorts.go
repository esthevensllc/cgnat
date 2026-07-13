package main

import (
	"sync"
	"sync/atomic"
)

const minimumCohortRetentionSeconds = 300

type insertCohort struct {
	Second int64  `json:"second"`
	Rows   uint64 `json:"rows"`
}

type cohortSnapshot struct {
	ReceivedPackets    uint64
	ParsedPackets      uint64
	ParseFailedPackets uint64
	ExpectedRows       uint64
	InsertedRows       uint64
	SpooledRows        uint64
	SpoolFailedRows    uint64
	UDPKernelDrops     uint64
}

type cohortBucket struct {
	mu sync.Mutex

	second int64

	receivedPackets    uint64
	parsedPackets      uint64
	parseFailedPackets uint64
	expectedRows       uint64
	insertedRows       uint64
	spooledRows        uint64
	spoolFailedRows    uint64
	udpKernelDrops     uint64
}

type cohortShard struct {
	buckets []cohortBucket
}

type cohortTracker struct {
	retentionSeconds int64
	shards           []cohortShard
}

func newCohortTracker(shardCount int, retentionSeconds int) *cohortTracker {
	if shardCount < 1 {
		shardCount = 1
	}
	if retentionSeconds < minimumCohortRetentionSeconds {
		retentionSeconds = minimumCohortRetentionSeconds
	}

	tracker := &cohortTracker{
		retentionSeconds: int64(retentionSeconds),
		shards:           make([]cohortShard, shardCount),
	}
	for index := range tracker.shards {
		tracker.shards[index].buckets = make([]cohortBucket, retentionSeconds)
	}
	return tracker
}

func (tracker *cohortTracker) shard(shardID int) *cohortShard {
	if shardID < 0 {
		shardID = -shardID
	}
	return &tracker.shards[shardID%len(tracker.shards)]
}

func (tracker *cohortTracker) bucket(second int64, shardID int) *cohortBucket {
	shard := tracker.shard(shardID)
	index := second % tracker.retentionSeconds
	if index < 0 {
		index += tracker.retentionSeconds
	}
	bucket := &shard.buckets[index]

	if atomic.LoadInt64(&bucket.second) == second {
		return bucket
	}

	bucket.mu.Lock()
	if atomic.LoadInt64(&bucket.second) != second {
		atomic.StoreUint64(&bucket.receivedPackets, 0)
		atomic.StoreUint64(&bucket.parsedPackets, 0)
		atomic.StoreUint64(&bucket.parseFailedPackets, 0)
		atomic.StoreUint64(&bucket.expectedRows, 0)
		atomic.StoreUint64(&bucket.insertedRows, 0)
		atomic.StoreUint64(&bucket.spooledRows, 0)
		atomic.StoreUint64(&bucket.spoolFailedRows, 0)
		atomic.StoreUint64(&bucket.udpKernelDrops, 0)
		atomic.StoreInt64(&bucket.second, second)
	}
	bucket.mu.Unlock()
	return bucket
}

func (tracker *cohortTracker) recordReceived(second int64, shardID int) {
	atomic.AddUint64(&tracker.bucket(second, shardID).receivedPackets, 1)
}

func (tracker *cohortTracker) recordParseResult(second int64, shardID int, parsed bool, expectedRows uint64) {
	bucket := tracker.bucket(second, shardID)
	if parsed {
		atomic.AddUint64(&bucket.parsedPackets, 1)
		atomic.AddUint64(&bucket.expectedRows, expectedRows)
		return
	}
	atomic.AddUint64(&bucket.parseFailedPackets, 1)
}

func (tracker *cohortTracker) recordInserted(cohorts []insertCohort, shardID int, nowSecond int64) {
	for _, cohort := range cohorts {
		if tracker.isRetained(cohort.Second, nowSecond) {
			atomic.AddUint64(&tracker.bucket(cohort.Second, shardID).insertedRows, cohort.Rows)
		}
	}
}

func (tracker *cohortTracker) recordSpooled(cohorts []insertCohort, shardID int, nowSecond int64) {
	for _, cohort := range cohorts {
		if tracker.isRetained(cohort.Second, nowSecond) {
			atomic.AddUint64(&tracker.bucket(cohort.Second, shardID).spooledRows, cohort.Rows)
		}
	}
}

func (tracker *cohortTracker) recordSpoolFailure(cohorts []insertCohort, shardID int, nowSecond int64) {
	for _, cohort := range cohorts {
		if tracker.isRetained(cohort.Second, nowSecond) {
			atomic.AddUint64(&tracker.bucket(cohort.Second, shardID).spoolFailedRows, cohort.Rows)
		}
	}
}

func (tracker *cohortTracker) recordUDPKernelDrops(second int64, shardID int, drops uint64) {
	if drops == 0 {
		return
	}
	atomic.AddUint64(&tracker.bucket(second, shardID).udpKernelDrops, drops)
}

func (tracker *cohortTracker) isRetained(second int64, nowSecond int64) bool {
	return second <= nowSecond+1 && second > nowSecond-tracker.retentionSeconds
}

func (tracker *cohortTracker) snapshot(startSecond int64, endSecond int64) cohortSnapshot {
	var result cohortSnapshot
	if endSecond < startSecond {
		return result
	}

	for second := startSecond; second <= endSecond; second++ {
		index := second % tracker.retentionSeconds
		if index < 0 {
			index += tracker.retentionSeconds
		}
		for shardIndex := range tracker.shards {
			bucket := &tracker.shards[shardIndex].buckets[index]
			if atomic.LoadInt64(&bucket.second) != second {
				continue
			}
			result.ReceivedPackets += atomic.LoadUint64(&bucket.receivedPackets)
			result.ParsedPackets += atomic.LoadUint64(&bucket.parsedPackets)
			result.ParseFailedPackets += atomic.LoadUint64(&bucket.parseFailedPackets)
			result.ExpectedRows += atomic.LoadUint64(&bucket.expectedRows)
			result.InsertedRows += atomic.LoadUint64(&bucket.insertedRows)
			result.SpooledRows += atomic.LoadUint64(&bucket.spooledRows)
			result.SpoolFailedRows += atomic.LoadUint64(&bucket.spoolFailedRows)
			result.UDPKernelDrops += atomic.LoadUint64(&bucket.udpKernelDrops)
		}
	}
	return result
}

func addInsertCohort(cohorts []insertCohort, second int64, rows uint64) []insertCohort {
	if rows == 0 {
		return cohorts
	}
	minimum := len(cohorts) - 4
	if minimum < 0 {
		minimum = 0
	}
	for index := len(cohorts) - 1; index >= minimum; index-- {
		if cohorts[index].Second == second {
			cohorts[index].Rows += rows
			return cohorts
		}
	}
	return append(cohorts, insertCohort{Second: second, Rows: rows})
}

func saturatingDifference(total uint64, completed uint64) uint64 {
	if completed >= total {
		return 0
	}
	return total - completed
}
