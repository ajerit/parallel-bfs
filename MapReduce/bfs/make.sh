#!/bin/bash
javac -classpath $HADOOP_CLASSPATH BFS.java
jar cvf bfs.jar BFS*.class

