# KIP 2018 | Apache Spark | Assignment 2

It is the solution of Apache Spark assignment 2 given during KIP 2018 sessions.
  
### Clone the repo

    git clone https://github.com/knoldus/spark-assignment-2.git
    cd spark-assignment-2

### Build the code 
If this is your first time running SBT, you will be downloading the internet.

    cd spark-assignment-2
    sbt clean compile

### Run
#### From Command Line
1.Set Environment Variables. Eg,

    export SPARK_MASTER="local"
    export SPARK_APP_NAME="kip"

2.Run `Spark Assignment 2 solution`

    cd /path/to/spark-assignment-2
    sbt run

After a few seconds you should be able to see data.
