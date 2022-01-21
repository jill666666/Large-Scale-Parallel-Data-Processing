# Makefile for Spark WordCount project.

# Customize these paths for your environment.
# -----------------------------------------------------------
spark.root=/Users/sunho/Dropbox/Boston/CS6240/tools/spark-3.1.2-bin-without-hadoop
hadoop.root=/Users/sunho/Dropbox/Boston/CS6240/tools/hadoop-3.3.0
app.name=Triangle Count
jar.name=spark-demo.jar
maven.jar.name=spark-demo-1.0.jar

# follower count job names
job.count.group.name=fc.RDDGroupMain
job.count.reduce.name=fc.RDDReduceMain
job.count.fold.name=fc.RDDFoldMain
job.count.aggregate.name=fc.RDDAggregateMain
job.count.dataset.name=fc.DSETMain

# triangle count job names
job.triangle.rep.r.name =tc.RepJoinRDDMain
job.triangle.rs.r.name =tc.RSJoinRDDMain
job.triangle.rep.d.name =tc.RepJoinDataFrameMain
job.triangle.rs.d.name =tc.RSJoinDataFrameMain

local.master=local[4]
local.input= /Users/sunho/Dropbox/Boston/CS6240/twitter-dataset/data/

# local follower count output directories
local.count.group.output=outputs/RDD-G
local.count.reduce.output=outputs/RDD-R
local.count.fold.output=outputs/RDD-F
local.count.aggregate.output=outputs/RDD-A
local.count.dataset.output=outputs/DSET

# local triangle count output directories
local.triangle.rep.r.output=outputs/Rep-R
local.triangle.rs.r.output=outputs/RS-R
local.triangle.rep.d.output=outputs/Rep-D
local.triangle.rs.d.output=outputs/RS-D

# Pseudo-Cluster Execution
hdfs.user.name=joe
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.emr.release=emr-6.4.0
aws.region=us-east-1
aws.bucket.name=hw3-spark-bucket
aws.input=input
aws.output=output
aws.log.dir=log
aws.num.nodes=8
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package
	cp target/${maven.jar.name} ${jar.name}

# # Removes local output directory.
# clean-local-output:
# 	rm -rf ${local.output}*

# Runs standalone
local-rdd-g: jar
	rm -rf ${local.count.group.output}
	spark-submit --class ${job.count.group.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.count.group.output}

local-rdd-r: jar
	rm -rf ${local.count.reduce.output}
	spark-submit --class ${job.count.reduce.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.count.reduce.output}

local-rdd-f: jar
	rm -rf ${local.count.fold.output}
	spark-submit --class ${job.count.fold.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.count.fold.output}

local-rdd-a: jar
	rm -rf ${local.count.aggregate.output}
	spark-submit --class ${job.count.aggregate.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.count.aggregate.output}

local-dset: jar
	rm -rf ${local.count.dataset.output}
	spark-submit --class ${job.count.dataset.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.count.dataset.output}

local-rep-r: jar
	rm -rf ${local.triangle.rep.r.output}
	spark-submit --class ${job.triangle.rep.r.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.triangle.rep.r.output}	

local-rs-r: jar
	rm -rf ${local.triangle.rs.r.output}
	spark-submit --class ${job.triangle.rs.r.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.triangle.rs.r.output}

local-rep-d: jar
	rm -rf ${local.triangle.rep.d.output}
	spark-submit --class ${job.triangle.rep.d.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.triangle.rep.d.output}

local-rs-d: jar
	rm -rf ${local.triangle.rs.d.output}
	spark-submit --class ${job.triangle.rs.d.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.triangle.rs.d.output}

# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs: 
	${hadoop.root}/sbin/stop-dfs.sh
	
# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.	
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.input}/* /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*

# Download output from HDFS to local.
download-output-hdfs:
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	spark-submit --class ${job.name} --master yarn --deploy-mode cluster ${jar.name} ${local.input} ${local.output}
	make download-output-hdfs

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output 
	spark-submit --class ${job.name} --master yarn --deploy-mode cluster ${jar.name} ${local.input} ${local.output}
	make download-output-hdfs

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.name} s3://${aws.bucket.name}

# Main EMR launch.
aws-rep-r: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "Rep-R 8 Workers Max 12000 Spark Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.triangle.rep.r.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate

aws-rs-r: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "RS-R 8 Workers Max 12000 Spark Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.triangle.rs.r.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate

aws-rep-d: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "Rep-D 8 Workers Max 12000 Spark Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.triangle.rep.d.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate

aws-rs-d: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "RS-D 8 Workers Max 12000 Spark Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.triangle.rs.d.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate
		
# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${local.output}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -f Spark-Demo.tar.gz
	rm -f Spark-Demo.zip
	rm -rf build
	mkdir -p build/deliv/Spark-Demo
	cp -r src build/deliv/Spark-Demo
	cp -r config build/deliv/Spark-Demo
	cp -r input build/deliv/Spark-Demo
	cp pom.xml build/deliv/Spark-Demo
	cp Makefile build/deliv/Spark-Demo
	cp README.txt build/deliv/Spark-Demo
	tar -czf Spark-Demo.tar.gz -C build/deliv Spark-Demo
	cd build/deliv && zip -rq ../../Spark-Demo.zip Spark-Demo
	