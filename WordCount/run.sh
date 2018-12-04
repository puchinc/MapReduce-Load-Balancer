# Hadoop and Java environment
export PATH=/hadoop-dist/target/hadoop-2.9.2/bin/:${PATH}
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
EXAMPLE=${1:-'WordCount'}
OUTPUT=${2:-'output'}

# Compile WordCount.java and create a jar:
hadoop com.sun.tools.javac.Main ${EXAMPLE}.java
jar cf ${EXAMPLE}.jar ${EXAMPLE}*.class

# Execute
hadoop jar ${EXAMPLE}.jar ${EXAMPLE} . ${OUTPUT}
rm -rf ${OUTPUT}
