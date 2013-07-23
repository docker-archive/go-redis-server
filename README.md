# go-redis-server: an implementation of the redis server protocol

There are plenty of good client implementations of the redis protocol, but not many *server* implementations.

go-redis-server is a helper library for building server software capable of speaking the redis protocol. This could be
an alternate implementation of redis, a custom proxy to redis, or even a completely different backend capable of
"masquerading" its API as a redis database.

Copyright (c) dotCloud 2013
