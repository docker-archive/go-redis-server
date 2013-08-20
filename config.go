package redis

type Config struct {
	proto   string
	host    string
	port    int
	handler interface{}
}

func DefaultConfig() *Config {
	return &Config{
		proto:   "tcp",
		host:    "127.0.0.1",
		port:    6389,
		handler: NewDefaultHandler(),
	}
}

func (c *Config) Port(p int) *Config {
	c.port = p
	return c
}

func (c *Config) Host(h string) *Config {
	c.host = h
	return c
}

func (c *Config) Proto(p string) *Config {
	c.proto = p
	return c
}

func (c *Config) Handler(h interface{}) *Config {
	c.handler = h
	return c
}
