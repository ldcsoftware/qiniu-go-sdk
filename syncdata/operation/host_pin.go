package operation

import (
	"sync/atomic"
	"time"
	"unsafe"
)

type pinnedHost struct {
	host   string
	expire int64 //unix nano
}

type HostPin struct {
	ph      *pinnedHost
	pinTime time.Duration
}

func NewHostPin(pinTimeMs int) *HostPin {
	return &HostPin{
		pinTime: time.Duration(pinTimeMs) * time.Millisecond,
	}
}

func (h *HostPin) Unpin() string {
	if h.pinTime == 0 {
		return ""
	}

	ch := (*pinnedHost)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&h.ph)), nil))
	if ch == nil {
		return ""
	}
	now := time.Now().UnixNano()
	if now > ch.expire {
		return ""
	}
	return ch.host
}

func (h *HostPin) Pin(host string) {
	if h.pinTime == 0 {
		return
	}
	ph := &pinnedHost{
		host:   host,
		expire: time.Now().Add(h.pinTime).UnixNano(),
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&h.ph)), unsafe.Pointer(ph))
}
