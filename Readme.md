
# Anomaly-temperature-detection-MRjobs-hadoop

You are given a dataset of temperature collected in major Australian cities from 1995 to 2020 (only January). Each record in the dataset consists of a city name, a date (month day year), and a temperature value (Fahrenheit). A sample input ﬁle has been provided. Your task is to utilize MRJob to detect anomalies from the statistics for each city

<img src = "https://miro.medium.com/v2/resize:fit:720/format:webp/1*q39d9M88lHxcHHwcuK8jsg.png"> </img>


## Run Locally

Clone the project

```bash
  git clone https://github.com/Rajadurai2/Anomaly-temperature-detection-MRjobs-hadoop
```

Go to the project directory

```bash
  cd my-project
```

Install hadoop 
- https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

Start hadoop
```bash
  $ hadoop_path/bin/start-all.sh
```

Run our program in hadoop cluster
```bash
  $ python3 project1.py -r hadoop testcase.txt -o hdfs_output --jobconf myjob.settings.tau=0.3 --jobconf
```

Run our program in local
```bash
  $ python3 project1.py -r local testcase.txt -o hdfs_output --jobconf myjob.settings.tau=0.3 --jobconf
```

Login to hadoop UI http://localhost:9870/index.html
- check output file in hdfs_output directory

