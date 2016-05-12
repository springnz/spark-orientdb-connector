# Spark OrientDB Connector

This library allows you to:

- access OrientDB classes as Spark RDDs
- access OrientDB graphs as GraphX graphs
- write RDDs to OrientDB
- write GraphX graphs to OrientDB

leveraging a co-located hybrid Spark/OrientDB cluster.

## How to get started

### Prerequisites

- sbt 0.13.x
- an OrientDB instance or several instances in distributed mode
- a Spark cluster

### Download source code and build connector

Clone connector repository with the following git commands:

```shell
	git clone https://github.com/metreta/spark-orientdb-connector.git
	cd spark-orientdb-connector
	git checkout tags/<tag_version>
```

where `<tag version>` is the version compatible with your Spark and OrientDB clusters you can find in the [version list](docs/version_list.md).

Then build the connector with sbt:

```shell
	sbt package
```

The default behaviour is to build for Scala 2.11. To build for Scala 2.10, execute the following instead

```shell
	sbt -Dscala-2.10=true package
```

### Create an sbt project and add dependencies

Create a basic Spark sbt project, add the connector jar you just built to the /lib folder and then add the following dependencies to your build.sbt:

```scala
  libraryDependencies ++= Seq(
    "com.orientechnologies" % "orientdb-client" % "2.1.0",
    "com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0",
    "org.apache.spark" %% "spark-core" % "1.5.1",
    "org.apache.spark" %% "spark-graphx" % "1.5.1"
    )
```

Don't forget to choose the appropriate library versions as listed in the [version list](docs/version_list.md).


### Set up example data in OrientDB

Define a class Person in your OrientDB instance:

```sql
CREATE CLASS Person EXTENDS V
CREATE PROPERTY Person.name string
CREATE PROPERTY Person.surname string
CREATE CLASS Friendship EXTENDS E
```

Insert some data to create a graph:
```sql
CREATE VERTEX Person SET name = 'John', surname = 'Doe'
CREATE VERTEX Person SET name = 'Mary', surname = 'Smith'
CREATE VERTEX Person SET name = 'Frank', surname = 'White'
CREATE VERTEX Person SET name = 'Lois', surname = 'Parker'

CREATE EDGE Friendship FROM (SELECT FROM Person WHERE name = 'John' and surname = 'Doe') TO (SELECT FROM Person WHERE name = 'Mary' and surname = 'Smith')
CREATE EDGE Friendship FROM (SELECT FROM Person WHERE name = 'John' and surname = 'Doe') TO (SELECT FROM Person WHERE name = 'Frank' and surname = 'White')
CREATE EDGE Friendship FROM (SELECT FROM Person WHERE name = 'Frank' and surname = 'White') TO (SELECT FROM Person WHERE name = 'Lois' and surname = 'Parker')
```

### Write an empty Spark app class

```scala
package com.mycompany.sparkorientdbdemo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.metreta.spark.orientdb.connector._

object Demo extends App{

// Spark app code

}

```


### Configure Spark context

Create a Spark configuration object and set OrientDB connector specific parameters  :

```scala
val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("OrientDBConnectorTest")
    .set("spark.orientdb.clustermode", "remote")
    .set("spark.orientdb.connection.nodes", "x.x.x.x, y.y.y.y, z.z.z.z")
    .set("spark.orientdb.protocol", "remote")
    .set("spark.orientdb.dbname", "connector-test")
    .set("spark.orientdb.port", "2424")
    .set("spark.orientdb.user", "root")
    .set("spark.orientdb.password", "pAzzw0rd")
```

Now create a SparkContext:

```scala
val sc = new SparkContext(conf)
```

### Read and write data

Following OrientDB multi-model approach the connector allows to access OrientDB data from Spark and write them back to OrientDB in two ways:

- Document approach: read OrientDB classes as Spark RDD and write an RDD to an Orient class
- Graph approach: read an OrientDB database as a GraphX graph object and write it back into a an OrientDB database

Use the function `orientQuery` to get an RDD containing all the entries of an OrientDB class:

```scala
 	val rddPeople = sc.orientQuery("Person")
```

`rddPeople` is an object of OrientClassRDD type containing  all the entries from the `Person` class as OrientDocument objects, created from the raw ODocument objects. If you prefer to get the ODocuments instead, use the following function:

```scala
 	val rddPeople = sc.orientDocumentQuery("Person")
```

The function `saveToOrient` writes an RDD of case objects to an OrientDB class:

```scala
	rddMyPeople.saveToOrient("Person")
```

To get a GraphX graph object from an OrientDB database use the `orientGraph` function:

```scala
	val graphPeople = sc.orientGraph()
```


## License

Copyright 2015, Metreta Information Technology s.r.l.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
