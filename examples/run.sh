# Hadoop and Java environment
export PATH=/hadoop-dist/target/hadoop-2.9.2/bin/:${PATH}
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

INPUT=${1:-'input'}
OUTPUT=${2:-'output'}
EXAMPLE=${3:-'WordCount'}

# Compile WordCount.java and create a jar:
hadoop com.sun.tools.javac.Main ${EXAMPLE}.java
jar cf ${EXAMPLE}.jar ${EXAMPLE}*.class

# Execute
hadoop jar ${EXAMPLE}.jar ${EXAMPLE} ${INPUT} ${OUTPUT}
find ${OUTPUT} -name 'part*' | xargs hadoop fs -cat
rm -rf ${OUTPUT}
