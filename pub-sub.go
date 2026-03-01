package main

import (
	"fmt"
	"net"
	"sync"
)

type Channel struct {
	mtx         sync.RWMutex
	subscribers map[net.Conn]bool
}

type SubscriberChannels struct {
	Channels map[string]*Channel
}

func NewChannel() *Channel {
	return &Channel{mtx: sync.RWMutex{}, subscribers: make(map[net.Conn]bool)}
}

// also returns true or false depending on if client
// was ALREADY subscribed to channel
func (c *Channel) AddSubscriber(conn net.Conn) bool {
	fmt.Println(conn)
	if exists := c.CheckMembership(conn); exists {
		return true
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.subscribers[conn] = true
	return false
}

func (c *Channel) RemoveSubscriber(conn net.Conn) bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	var ok bool
	if _, ok = c.subscribers[conn]; ok {
		delete(c.subscribers, conn)
	}

	return ok
}

func (c *Channel) PublishMessage(msg []byte) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	for conn := range c.subscribers {
		// fmt.Println(string(msg))
		go func() {
			conn.Write(msg)
		}() // execute in go func to not wait on any slow clients
	}
}

func (c *Channel) PublishDirectMessage(msg []byte, conn net.Conn) {
	conn.Write(msg)
}

func (c *Channel) CheckMembership(conn net.Conn) bool {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	_, ok := c.subscribers[conn]

	return ok
}

func (s *SubscriberChannels) AddSubscriber(conn net.Conn, chanName string) bool {
	channel, ok := s.Channels[chanName]
	if !ok {
		s.Channels[chanName] = NewChannel()
		channel = s.Channels[chanName]
	}

	return channel.AddSubscriber(conn)
}

func (s *SubscriberChannels) RemoveSubscriber(conn net.Conn, chanName string) {
	channel, ok := s.Channels[chanName]
	if !ok {
		return
	}

	channel.RemoveSubscriber(conn)
}

func (s *SubscriberChannels) PublishMessage(msg []byte, chanName string) {
	channel, ok := s.Channels[chanName]
	if !ok {
		return
	}

	channel.PublishMessage(msg)
}

func (s *SubscriberChannels) PublishDirectMessage(msg []byte, chanName string, conn net.Conn) {
	channel, ok := s.Channels[chanName]
	if !ok {
		return
	}

	channel.PublishDirectMessage(msg, conn)
}

func (s *SubscriberChannels) GetNumSubscribedChan(conn net.Conn) uint64 {
	count := uint64(0)
	for _, channel := range s.Channels {
		if exists := channel.CheckMembership(conn); exists {
			count++
		}
	}

	return count
}
