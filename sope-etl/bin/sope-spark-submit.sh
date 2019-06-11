#!/usr/bin/env bash

if [ -z "${SPARK_HOME}" ]; then
  source "$(dirname "$0")"/find-spark-home
fi

. "${SPARK_HOME}"/bin/load-spark-env.sh

# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
else
  if [ "$(command -v java)" ]; then
    RUNNER="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

# Find Spark jars.
if [ -d "${SPARK_HOME}/jars" ]; then
  SPARK_JARS_DIR="${SPARK_HOME}/jars"
else
  SPARK_JARS_DIR="${SPARK_HOME}/assembly/target/scala-$SPARK_SCALA_VERSION/jars"
fi

if [ ! -d "$SPARK_JARS_DIR" ] && [ -z "$SPARK_TESTING$SPARK_SQL_TESTING" ]; then
  echo "Failed to find Spark jars directory ($SPARK_JARS_DIR)." 1>&2
  echo "You need to build Spark with the target \"package\" before running this program." 1>&2
  exit 1
else
  LAUNCH_CLASSPATH="$SPARK_JARS_DIR/*"
fi

# Add the launcher build dir to the classpath if requested.
if [ -n "$SPARK_PREPEND_CLASSES" ]; then
  LAUNCH_CLASSPATH="${SPARK_HOME}/launcher/target/scala-$SPARK_SCALA_VERSION/classes:$LAUNCH_CLASSPATH"
fi

CMD_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
LAUNCH_CLASSPATH="$CMD_DIR/lib/*:$LAUNCH_CLASSPATH"


"$RUNNER" -Xmx128m -cp "$LAUNCH_CLASSPATH" com.sope.etl.utils.WrapperUtil "$@"
status=$?
if [ $status -ne 0 ]
then
 exit -1
fi

OPT_ARR=()
while IFS= read -r line; do
    OPT_ARR+=( "$line" )
done < <( "$RUNNER" -Xmx128m -cp "$LAUNCH_CLASSPATH" com.sope.etl.utils.WrapperUtil "$@" )
IFS='|' read -r -a SPARK_OPTS <<< ${OPT_ARR[0]}
IFS='|' read -r -a SOPE_OPTS <<< ${OPT_ARR[1]}


SPARK_CMD=(${SPARK_HOME}/bin/spark-submit  "${SPARK_OPTS[@]}" --class  com.sope.etl.YamlRunner  $CMD_DIR/lib/sope-etl*.jar "${SOPE_OPTS[@]}")
exec "${SPARK_CMD[@]}"