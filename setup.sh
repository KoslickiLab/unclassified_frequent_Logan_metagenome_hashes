#conda activate logan
export JAVA_HOME="$CONDA_PREFIX"           # conda openjdk home
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_LOCAL_DIRS="/scratch/spark_local"

