# MonteCarloArea
Example framework used in [Apache Mesos Essentials book](http://dharmeshkakadia.blogspot.com/2015/06/apache-mesos-essential-is-now-available.html)

## Run with docker-compose
To try the framework with [docker compose](https://www.docker.com/docker-compose) use the following steps after installing docker-compose: 

1. Clone this repo using
    ```shell
    git clone https://github.com/dharmeshkakadia/MonteCarloArea/ && cd MonteCarloArea
    ```

2. Lets create a Mesos + Marathon cluster using Mesosphere docker images:
    ```shell
    docker-compose up -d
    ```

3. Now lets compile our code and create jar. Note that the ``src`` directory is sharted under ``/tmp/data`` in all the containers.
    ```shell
    docker exec  montecarloarea_marathon_1 bash -c "javac -cp \"/usr/share/java/*\" /tmp/data/org/packt/mesos/*.java && cd /tmp/data/; jar -cvf /tmp/data/MonteCarloArea.jar  org/packt/mesos/*"
    ```

4. Now lets run it !
    ```shell
    docker exec  montecarloarea_marathon_1 bash -c "java -cp /tmp/data/MonteCarloArea.jar:/usr/share/java/* org.packt.mesos.App zk://zk:2181/mesos 4  x 0 10 0 10 10" 
    ```

