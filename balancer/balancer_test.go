package balancer

import (
	"testing"

	"github.com/BurntSushi/toml"
)

func TestBalancer(t *testing.T) {
	config := BalancerConfig{}
	if _, err := toml.DecodeFile("./balancer_config.toml", &config); err != nil {
		t.Fatal(err)
	}

	b := NewRoundRobinBalancer(&config)
	for index := 0; index < 10; index++ {
		ep, err := b.Get()
		if err != nil {
			t.Fatal(err)
		}
		t.Log(ep.Name)
	}
}
