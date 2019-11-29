#! bin/bash
set -x
USER=$1
JUPYTER_HOST=*
JUPYTER_PORT=8889
su - ${USER} << EOF 
export SPARK_HOME=/usr/hdp/current/spark2-client
export PYSPARK_SUBMIT_ARGS="--master yarn-client pyspark-shell"
export HADOOP_HOME=/usr/hdp/current/hadoop-client
export HADOOP_CONF_DIR=/usr/hdp/current/hadoop-client/conf
export PYTHONPATH="/usr/hdp/current/spark2-client/python:/usr/hdp/current/spark2-client/python/lib/py4j-0.9-src.zip"
export PYTHONSTARTUP=/usr/hdp/current/spark2-client/python/pyspark/shell.py
export PYSPARK_SUBMIT_ARGS="--master yarn-client pyspark-shell"
echo "Starting Jupyter daemon on HDP Cluster ..."
#jupyter notebook --allow-root --notebook-dir='/root/capstone' --config=/root/jupyter-conf/jupyter_notebook_config.py --ip=${JUPYTER_HOST} --port=${JUPYTER_PORT}&
PYSPARK_DRIVER_PYTHON="jupyter" PYSPARK_DRIVER_PYTHON_OPTS="notebook" pyspark
EOF
exit 0
