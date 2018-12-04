# Setup MapReduce container environment and execute mapreduce example
EXAMPLE=${1:-'WordCount'}

docker run --rm=true -it \
    -v $(pwd)/hadoop-dist:/hadoop-dist \
    -v $(pwd)/${EXAMPLE}:/${EXAMPLE} \
    openjdk \
    bash -c "cd ${EXAMPLE}; bash run.sh"
