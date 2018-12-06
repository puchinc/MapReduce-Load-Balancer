# Setup MapReduce container environment and execute mapreduce example
INPUT=${1:-'small_input'}
OUTPUT=${2:-'output'}
EXAMPLE=${3:-'WordCount'}

docker run --rm=true -it \
    -v $(pwd)/hadoop-dist:/hadoop-dist \
    -v $(pwd)/examples:/examples \
    openjdk \
    bash -c "cd examples; bash run.sh ${INPUT} ${OUTPUT} ${EXAMPLE}"
