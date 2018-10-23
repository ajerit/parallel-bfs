#!/bin/bash
javac -classpath $HADOOP_CLASSPATH FormatInput.java
jar cvf formatInput.jar FormatInput*.class
