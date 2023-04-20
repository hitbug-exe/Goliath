# Goliath

Goliath is a high-performace, monolithic load balancer written in Go. It implements various load balancing policies (Round Robin, Weighted Round Robin, Random, URL Hashing, and Least Connections). It can load balance traffic to a list of backend servers and handles health checks to detect and remove unhealthy servers from the pool.

# Installation

To install this load balancer, you need to have Go installed on your machine. Then you can clone the repository and build the binary by running the following commands:

  `
  git clone https://github.com/hitbug-exe/Goliath.git
  cd load-balancer
  go build
  `

# Usage

To use the load balancer, you need to provide a list of backend servers as a comma-separated list using the `-backends` flag. You can also provide a custom port to serve traffic using the `-port` flag, and a load balancing policy using the `-policy` flag. The supported policies are:

  * `rr`: Round Robin
  * `wrr`: Weighted Round Robin
  * `rand`: Random
  * `uhash`: URL Hashing
  * `lc`: Least Connections

Here is an example command to start the load balancer with two backend servers using the Round Robin policy:

  `./goliath -backends=http://localhost:8080,http://localhost:8081 -policy=rr`

# Documentation

The code is well-documented with inline and multiline comments that explain the purpose of each function, the arguments it takes, and the values it returns. The documentation also includes example use cases for each function.

# License

Goliath is licensed under the MIT License. Feel free to break things with it:)