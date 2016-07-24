# MonteCarloArea
Example framework used in [Apache Mesos Essentials book](http://dharmeshkakadia.blogspot.com/2015/06/apache-mesos-essential-is-now-available.html). Current master of this repo uses latest version of Mesos. If you want to see the version used in the book, look at [branch-0.23](https://github.com/dharmeshkakadia/MonteCarloArea/tree/0.23-book).

## Run with docker-compose
To try the framework with [docker compose](https://www.docker.com/docker-compose) use the following steps after installing docker-compose: 

[![asciicast](https://asciinema.org/a/bc55iq91i7emlowx3j77280fo.png)](https://asciinema.org/a/bc55iq91i7emlowx3j77280fo)

1. Clone this repo using
    ```shell
    git clone https://github.com/dharmeshkakadia/MonteCarloArea/ && cd MonteCarloArea
    ```

2. Compile our code and create a jar.
    ```shell
    mvn package
    ```
3. Lets create a Mesos cluster using Mesosphere docker images. Mesos web interface will be available at [http://localhost:5050](http://localhost:5050). (If you are using boot2docker substitute the IP of the docker VM). Note that the ``target`` directory from the project root is mounted under ``/tmp/bin`` in all Mesos master and slave containers.
    ```shell
    docker-compose up -d
    ```

4. Now lets run it ! You should see the area calculated in the output.
    ```shell
    docker exec  montecarloarea_master_1 bash -c "java -cp /tmp/bin/MonteCarloArea-1.0-SNAPSHOT.jar org.packt.mesos.App zk://zk:2181/mesos 4  x 0 10 0 10 1000"
    ```

5. You can stop the Mesos cluster, using
    ```shell
    docker-compose stop
    ```
