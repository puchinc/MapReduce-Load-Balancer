#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e               # exit on error

cd "$(dirname "$0")" # connect to root

docker build -t hadoop-build dev-support/docker

if [ "$(uname -s)" == "Linux" ]; then
  USER_NAME=${SUDO_USER:=$USER}
  USER_ID=$(id -u "${USER_NAME}")
  GROUP_ID=$(id -g "${USER_NAME}")
else # boot2docker uid and gid
  USER_NAME=$USER
  USER_ID=1000
  GROUP_ID=50
fi

docker build -t "hadoop-build-${USER_ID}" - <<UserSpecificDocker
FROM hadoop-build
RUN groupadd --non-unique -g ${GROUP_ID} ${USER_NAME}
RUN useradd -g ${GROUP_ID} -u ${USER_ID} -k /root -m ${USER_NAME}
ENV HOME /home/${USER_NAME}
UserSpecificDocker

# By mapping the .m2 directory you can do an mvn install from
# within the container and use the result on your normal
# system.  And this also is a significant speedup in subsequent
# builds because the dependencies are downloaded only once.

# compile in the container

# First time compilation

if [ ! -d hadoop-cloud-storage-project/target ]; then
    DIRS=( . )
else
    SRC=${1:-'hadoop-mapreduce-project'}
    DIRS=( ${SRC} hadoop-dist )
fi

COMPILE='mvn package -Pdist -DskipTests -Dtar -Dmaven.javadoc.skip=true'
for DIR in "${DIRS[@]}" 
do
    docker run --rm=true -t -i \
      -v "${PWD}:/home/${USER_NAME}/hadoop" \
      -w "/home/${USER_NAME}/hadoop" \
      -v "${HOME}/.m2:/home/${USER_NAME}/.m2" \
      -u "${USER_NAME}" \
      "hadoop-build-${USER_ID}" \
      bash -c "cd ${DIR}; ${COMPILE}"
done
