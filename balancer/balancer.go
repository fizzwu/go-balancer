// A Load Balancer
// Support two balance policies: Round Robin, Smooth Weighted Round Robin
// Round Robin:
// 	basic polling policy, get backend endpoint one by one
// Smooth Weighted Round Robin:
// 	Algorithm:
// 	1. In each loop, increase every endpoint's current weight by its weight
// 	2. Select the endpoint with the greatest current weight, reduce current weight by total
// 	Example:
// 	endpoints: {A:3, B:2, C:1}, loop like this:

// 	A		B		C		Select
// 	3		2		1		A
// 	3+3-6	1+2		1+1
// 	0		3		2		B
// 	0+3		3+2-6	2+1
// 	3		-1		3		A
// 	3+3-6	-1+2	3+1
// 	0		1		4		C
// 	...

package balancer

import (
	"fmt"
	"sync"
)

const (
	RoundRobinBasic          = 0
	WeightedRoundRobinSmooth = 1
)

type BalancerConfig struct {
	Algor     string            `toml:"algor"`
	Endpoints []*EndpointConfig `toml:"endpoints"`
}

type EndpointConfig struct {
	Name   string `toml:"name"`
	Addr   string `toml:"addr"`
	Weight int    `toml:"weight"`
}

type Endpoint struct {
	Name string
	Addr string
}

func (e *Endpoint) Available() bool {
	// TODO
	return true
}

type RoundRobinEndpoint struct {
	weight   int
	current  int
	endpoint *Endpoint
}

type RoundRobinCluster []*RoundRobinEndpoint

type RoundRobinBalancer struct {
	sync.Mutex
	algor   int
	cluster RoundRobinCluster
	next    int
}

func NewRoundRobinBalancer(config *BalancerConfig) *RoundRobinBalancer {
	rrEndpoints := []*RoundRobinEndpoint{}
	for _, c := range config.Endpoints {
		endpoint := &Endpoint{
			Name: c.Name,
			Addr: c.Addr,
		}
		rrEndpoints = append(rrEndpoints, &RoundRobinEndpoint{
			weight:   c.Weight,
			endpoint: endpoint,
		})
	}
	var algor int
	switch config.Algor {
	case "round_robin":
		algor = RoundRobinBasic
	case "weighted_round_robin":
		algor = WeightedRoundRobinSmooth
	default:
		algor = WeightedRoundRobinSmooth
	}
	b := &RoundRobinBalancer{
		algor:   algor,
		cluster: rrEndpoints,
	}
	return b
}

func (b *RoundRobinBalancer) Get() (*Endpoint, error) {
	switch b.algor {
	case RoundRobinBasic:
		return b.getByRoundRobin()
	case WeightedRoundRobinSmooth:
		return b.getByWeightedRoundRobin()
	default:
		return b.getByWeightedRoundRobin()
	}

}

func getNext(n int, length int) int {
	n++
	if n >= length {
		n = 0
	}
	return n
}

func (b *RoundRobinBalancer) getByRoundRobin() (*Endpoint, error) {
	b.Lock()
	defer b.Unlock()

	var endpointRR *RoundRobinEndpoint
	var endpoint *Endpoint
	next := b.next
	for {
		endpointRR = b.cluster[next]
		endpoint = endpointRR.endpoint
		if endpointRR.weight > 0 && endpoint.Available() {
			break
		}
		next = getNext(next, len(b.cluster))
		if next == b.next {
			return nil, fmt.Errorf("All endpoints unavailable")
		}
	}
	b.next = getNext(next, len(b.cluster))

	return endpoint, nil
}

func (b *RoundRobinBalancer) getByWeightedRoundRobin() (*Endpoint, error) {
	b.Lock()
	defer b.Unlock()

	var best *RoundRobinEndpoint
	total, max := 0, 0
	for _, endpointRR := range b.cluster {
		if endpointRR.weight <= 0 || !endpointRR.endpoint.Available() {
			continue
		}
		if best == nil || endpointRR.current > max {
			best = endpointRR
			max = endpointRR.current
		}
		total += endpointRR.current
		endpointRR.current += endpointRR.weight
	}

	if best == nil {
		return nil, fmt.Errorf("No endpoint available")
	}

	best.current -= total

	return best.endpoint, nil
}
