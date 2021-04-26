package operation

import (
	"sync"
	"time"
)

type selHost struct {
	host   string
	expire int64
}

type HostSelector struct {
	hosts        []selHost
	update       func() []string
	updateTimeS  int
	punishTimeS  int
	shouldPunish func(error) bool
	mutex        sync.RWMutex
	punishHostM  map[string]int64
	idx          int
}

func NewHostSelector(hosts []string, update func() []string, updateTimeS, punishTimeS int, shouldPunish func(error) bool) *HostSelector {
	if punishTimeS == 0 {
		punishTimeS = 30
	}
	if updateTimeS == 0 {
		updateTimeS = 300
	}

	hs := &HostSelector{
		update:       update,
		updateTimeS:  updateTimeS,
		punishTimeS:  punishTimeS,
		shouldPunish: shouldPunish,
	}
	hs.setHosts(hosts)
	hs.hostUpdate()
	go func() {
		for {
			time.Sleep(time.Duration(hs.updateTimeS) * time.Second)
			hs.hostUpdate()
		}
	}()
	return hs
}

func (hs *HostSelector) hostUpdate() {
	newHosts := hs.update()
	if len(newHosts) > 0 {
		hs.setHosts(newHosts)
	}
}

func (hs *HostSelector) setHosts(hosts []string) {
	elog.Info("update host", hosts)

	hs.mutex.Lock()
	defer hs.mutex.Unlock()

	selHosts := make([]selHost, len(hosts))
	for i := range hosts {
		var expire int64
		for j := range hs.hosts {
			if hosts[i] == hs.hosts[j].host {
				expire = hs.hosts[j].expire
				break
			}
		}
		selHosts[i] = selHost{host: hosts[i], expire: expire}
	}
	hs.hosts = selHosts
}

func (hs *HostSelector) SelectHost() string {
	hs.mutex.RLock()
	defer hs.mutex.RUnlock()

	if len(hs.hosts) == 0 {
		return ""
	}
	tryTime := 0
	now := time.Now().UnixNano()
	for {
		hs.idx += 1
		tryTime += 1
		currHost := hs.hosts[hs.idx%len(hs.hosts)]
		if tryTime > len(hs.hosts) || now >= currHost.expire {
			return currHost.host
		}
	}
}

func (hs *HostSelector) SetPunish(host string) {
	hs.mutex.Lock()
	defer hs.mutex.Unlock()

	for i := range hs.hosts {
		if hs.hosts[i].host == host {
			hs.hosts[i].expire = time.Now().Add(time.Duration(hs.punishTimeS) * time.Second).UnixNano()
			break
		}
	}
}

func (hs *HostSelector) IsPunished(host string) bool {
	hs.mutex.Lock()
	defer hs.mutex.Unlock()

	for i := range hs.hosts {
		if hs.hosts[i].host == host {
			return time.Now().UnixNano() < hs.hosts[i].expire
		}
	}
	return false
}

func (hs *HostSelector) SetFailed(host string, err error) {
	if hs.shouldPunish(err) {
		hs.SetPunish(host)
	}
}
