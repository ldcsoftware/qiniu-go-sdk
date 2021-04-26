package operation

import (
	"fmt"
	"testing"

	"github.com/qiniupd/qiniu-go-sdk/x/log.v7"
)

func checkHost(host string, hosts []string) bool {
	for i := range hosts {
		if host == hosts[i] {
			return true
		}
	}
	return false
}

func checkHostCount(t *testing.T, hosts []string, hostStat map[string]int, count int) {
	for i := range hosts {
		if hostStat[hosts[i]] != count {
			t.Fatalf("check host failed. stat:%v count:%v", hostStat[hosts[i]], count)
		}
	}
}

func checkTrue(t *testing.T, param bool) {
	if !param {
		t.Fatal("check true failure")
	}
}

func TestSelectorPunish(t *testing.T) {
	hosts1 := []string{"host1", "host2", "host3"}
	hostStat := make(map[string]int)

	update := func() []string {
		return nil
	}
	hs := NewHostSelector(hosts1, update, 0, 0, shouldRetry)

	tryTime := 9
	for i := 0; i < tryTime; i++ {
		host := hs.SelectHost()
		hostStat[host] = hostStat[host] + 1
	}
	log.Infof("step 1 hostStat:%v", hostStat)
	checkHostCount(t, hosts1, hostStat, tryTime/len(hosts1))

	hs.SetFailed("host1", nil)
	checkTrue(t, !hs.IsPunished("host1"))
	hs.SetFailed("host1", fmt.Errorf("internal error"))
	checkTrue(t, hs.IsPunished("host1"))

	hostStat = make(map[string]int)
	tryTime = 10
	for i := 0; i < tryTime; i++ {
		host := hs.SelectHost()
		hostStat[host] = hostStat[host] + 1
	}
	hosts2 := []string{"host2", "host3"}
	log.Infof("step 2 hostStat:%v", hostStat)
	checkHostCount(t, hosts2, hostStat, tryTime/len(hosts2))

	hs.SetFailed("host2", fmt.Errorf("internal error"))
	hs.SetFailed("host3", fmt.Errorf("internal error"))
	checkTrue(t, hs.IsPunished("host2"))
	checkTrue(t, hs.IsPunished("host3"))

	hostStat = make(map[string]int)
	tryTime = 9
	for i := 0; i < tryTime; i++ {
		host := hs.SelectHost()
		hostStat[host] = hostStat[host] + 1
	}
	log.Infof("step 3 hostStat:%v", hostStat)
	checkHostCount(t, hosts1, hostStat, tryTime/len(hosts1))
}

func TestSelectorUpdate(t *testing.T) {
	hosts1 := []string{"host1"}
	hosts2 := []string{"host1", "host2", "host3"}

	var updateCtrl bool = false
	update := func() []string {
		if !updateCtrl {
			return nil
		}
		return hosts2
	}
	hs := NewHostSelector(hosts1, update, 0, 0, shouldRetry)

	hs.SetPunish("host1")
	checkTrue(t, hs.IsPunished("host1"))
	log.Infof("hs.SelectHost:%v", hs.SelectHost())
	checkTrue(t, hs.SelectHost() == "host1")

	updateCtrl = true
	hs.hostUpdate()
	checkTrue(t, hs.IsPunished("host1"))

	hostStat := make(map[string]int)
	tryTime := 10
	for i := 0; i < tryTime; i++ {
		host := hs.SelectHost()
		hostStat[host] = hostStat[host] + 1
	}
	hosts3 := []string{"host2", "host3"}
	log.Infof("step 2 hostStat:%v", hostStat)
	checkHostCount(t, hosts3, hostStat, tryTime/len(hosts3))
}
