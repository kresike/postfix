package postfix

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RatelimitToken holds data for one sender about the amount of recently sent mails and is protected by a mutex
type RatelimitToken struct {
	mu         sync.Mutex
	key        string
	tsd        map[time.Time]int
	count      int
	sliceCount int
	logger     *log.Logger
}

// RatelimitTokenMap holds all the sender's tokens protected by a Mutex
type RatelimitTokenMap struct {
	mu     sync.Mutex
	tokens map[string]*RatelimitToken
	logger *log.Logger
}

// RatelimitSlidingWindow is a data structure that holds all information necessary to make a decision whether to allow or block an email
type RatelimitSlidingWindow struct {
	mu           sync.Mutex
	defaultLimit int
	deferMessage string "rate limit exceeded"
	interval     time.Duration
	whiteList    *MemoryMap
	domainList   *MemoryMap
	tokens       *RatelimitTokenMap
	logger       *log.Logger
}

// NewRatelimitSlidingWindow creates a structure of type RatelimitSlidingWindow
func NewRatelimitSlidingWindow(w, d *MemoryMap, t *RatelimitTokenMap) *RatelimitSlidingWindow {
	var rsw RatelimitSlidingWindow
	rsw.defaultLimit = 120
	rsw.whiteList = w
	rsw.domainList = d
	rsw.tokens = t

	return &rsw
}

// NewRatelimitTokenMap creates a structure of type RatelimitTokenMap
func NewRatelimitTokenMap() *RatelimitTokenMap {
	var rt RatelimitTokenMap
	rt.tokens = make(map[string]*RatelimitToken)
	return &rt
}

// NewRatelimitToken creates a structure of type RatelimitToken
func NewRatelimitToken(k string) *RatelimitToken {
	var t RatelimitToken
	t.tsd = make(map[time.Time]int)
	t.key = k
	t.count = 0
	t.sliceCount = 0

	return &t
}

// SetDefaultLimit sets the rate limit for domains not listed in the domain list and not whitelisted
func (rsw *RatelimitSlidingWindow) SetDefaultLimit(l int) {
	rsw.mu.Lock()
	defer rsw.mu.Unlock()
	rsw.defaultLimit = l
}

// SetInterval sets the window interval that the limit applies to
func (rsw *RatelimitSlidingWindow) SetInterval(i string) {
	rsw.mu.Lock()
	defer rsw.mu.Unlock()
	d, err := time.ParseDuration(i + "s")
	if err != nil {
		rsw.logger.Println("Failed to parse duration", i)
	}
	rsw.interval = d * -1
}

// SetLogger sets the logger on the RatelimitSlidingWindow
func (rsw *RatelimitSlidingWindow) SetLogger(l *log.Logger) {
	rsw.mu.Lock()
	defer rsw.mu.Unlock()
	rsw.logger = l
}

// SetLogger sets the logger on the RatelimitTokenMap
func (rlm *RatelimitTokenMap) SetLogger(l *log.Logger) {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()
	rlm.logger = l
}

// SetLogger sets the logger on the RatelimitToken
func (rlt *RatelimitToken) SetLogger(l *log.Logger) {
	rlt.mu.Lock()
	defer rlt.mu.Unlock()
	rlt.logger = l
}

// SetDeferMessage sets the defer message sent to the client in case the limit is exceeded
func (rsw *RatelimitSlidingWindow) SetDeferMessage(m string) {
	rsw.mu.Lock()
	defer rsw.mu.Unlock()
	rsw.deferMessage = m
}

// SetWhiteList sets the white list
func (rsw *RatelimitSlidingWindow) SetWhiteList(wl *MemoryMap) {
	rsw.mu.Lock()
	defer rsw.mu.Unlock()
	rsw.whiteList = wl
}

// SetDomainList sets the domain list
func (rsw *RatelimitSlidingWindow) SetDomainList(d *MemoryMap) {
	rsw.mu.Lock()
	defer rsw.mu.Unlock()
	rsw.domainList = d
}

func (rsw RatelimitSlidingWindow) checkWhiteList(k string) bool {
	if _, err := rsw.whiteList.Get(k); err != nil {
		return false
	}
	return true
}

func (rsw RatelimitSlidingWindow) checkDomain(k string) bool {
	if _, err := rsw.domainList.Get(k); err != nil {
		return false
	}
	return true
}

func (rsw RatelimitSlidingWindow) getDomainLimit(dom string) int {
	d, err := rsw.domainList.Get(dom)
	if err != nil {
		rsw.logger.Println("Failed to get domain data for:", dom)
		return 0
	}
	val, err := strconv.Atoi(d)
	if err != nil {
		rsw.logger.Println("Cannot convert value ", d, " to int")
		return 0
	}
	return val
}

// RateLimit checks whether a sender can send the message and returns the appropriate postfix policy action string
func (rsw RatelimitSlidingWindow) RateLimit(sender string, recips int) string {
	rsw.mu.Lock()
	defer rsw.mu.Unlock()
	elems := strings.Split(sender, "@")
	//	user := elems[0] // the user part of sender
	domain := "" // domain defaults to empty
	messagelimit := rsw.defaultLimit
	if len(elems) > 1 {
		domain = elems[1] // the domain part of sender
	}

	if recips == 0 {
		rsw.logger.Println("Recipients is 0, increasing to 1")
		recips++
	}

	if rsw.checkWhiteList(sender) {
		rsw.logger.Println("Allowing whitelisted sender:", sender)
		return "action=dunno\n\n" // permit whitelisted sender
	}
	if rsw.checkWhiteList(domain) {
		rsw.logger.Println("Allowing whitelisted domain:", domain)
		return "action=dunno\n\n" // permit whitelisted domain
	}
	if rsw.checkDomain(domain) {
		messagelimit = rsw.getDomainLimit(domain)
	}

	token := rsw.tokens.Token(sender)

	now := time.Now()

	limit := now.Add(rsw.interval)

	token.Prune(limit)
	tcount := token.Count() + recips

	if tcount > messagelimit {
		rsw.logger.Println("Message from", sender, "rejected, limit", messagelimit, "reached (", tcount, ")")
		return "action=defer_if_permit " + rsw.deferMessage + "\n\n"
	}

	token.RecordMessage(now, recips)

	rsw.logger.Println("Message accepted from", sender, "recipients", recips, "current", token.Count(), "limit", messagelimit)
	return "action=dunno\n\n"
}

// AddToken adds a new token to a RatelimitTokenMap
func (rlm RatelimitTokenMap) AddToken(t *RatelimitToken) {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()
	rlm.tokens[t.Key()] = t
}

// Token returns a token from a RatelimitTokenMap
func (rlm RatelimitTokenMap) Token(k string) *RatelimitToken {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()
	if t, ok := rlm.tokens[k]; ok {
		return t
	} else {
		t := NewRatelimitToken(k)
		t.SetLogger(rlm.logger)
		rlm.tokens[k] = t
		return t
	}
}

// Key returns the key of a RatelimitToken
func (rlt *RatelimitToken) Key() string {
	rlt.mu.Lock()
	defer rlt.mu.Unlock()
	return rlt.key
}

// RecordMessage records a message in the RatelimitToken by updating or adding a timeslice
func (rlt *RatelimitToken) RecordMessage(ts time.Time, recips int) {
	rlt.mu.Lock()
	defer rlt.mu.Unlock()
	keytime := ts.Truncate(time.Minute)
	rlt.logger.Println("Recording message for", rlt.key, "count:", rlt.count, "slices:", rlt.sliceCount, "time:", keytime, "recipients:", recips)
	if val, ok := rlt.tsd[keytime]; ok {
		rlt.count += recips
		rlt.tsd[keytime] = val + recips
	} else {
		rlt.count += recips
		rlt.sliceCount++
		rlt.tsd[keytime] = recips
	}
}

// Count returns the number of messages currently in the Token, make sure to call Prune before calling this
func (rlt *RatelimitToken) Count() int {
	rlt.mu.Lock()
	defer rlt.mu.Unlock()
	return rlt.count
}

// Prune clears all expired time slices from a RatelimitToken
func (rlt *RatelimitToken) Prune(lim time.Time) {
	rlt.mu.Lock()
	defer rlt.mu.Unlock()
	for t, val := range rlt.tsd {
		if t.Before(lim) {
			rlt.logger.Println("Pruning", rlt.key, "slice with key:", t, "containing", val, "entries")
			rlt.count -= val
			rlt.sliceCount--
			delete(rlt.tsd, t)
		}
	}
}

// String is a simple stringer for the RatelimitToken
func (rlt *RatelimitToken) String() string {
	s := fmt.Sprintf("RatelimitToken: %s count %d slices %d", rlt.key, rlt.count, rlt.sliceCount)
	return s
}
