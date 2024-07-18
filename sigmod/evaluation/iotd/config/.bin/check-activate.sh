#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

source "$(dirname "$0")/iotdb-common.sh"
checkAllConfigNodeVariables
initConfigNodeEnv
LICENSE_PATH="$CONFIGNODE_HOME/activation/license"
main_class=com.timecho.iotdb.manager.activation.ActivationVerifier
iotdb_params="-DCONFIGNODE_CONF=${CONFIGNODE_CONF}"
iotdb_params="$iotdb_params -DCONFIGNODE_HOME=${CONFIGNODE_HOME}"
CLASSPATH=""
for f in "${CONFIGNODE_HOME}"/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done
exec "$JAVA" $iotdb_params -cp "$CLASSPATH" "$main_class" $LICENSE_PATH
