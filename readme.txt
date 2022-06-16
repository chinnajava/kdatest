1) maven setup
export M2_HOME=/Users/chinna/Downloads/apache-maven-3.8.5
export M2=$M2_HOME/bin
export PATH=$M2:$PATH


2) install file
mvn clean package


3) Sample event

{"event":{"version":"3","account-id":"100000674","interface-id":"eni-1","srcaddr":"1.72.117.201","dstaddr":"1.63.248.69","srcport":"47213","dstport":"53","protocol":"17","packets":"1","bytes":"73","start":"1655392116","end":"1655392117","action":"ACCEPT","log-status":"OK","vpc-id":"vpc-1","subnet-id":"subnet-1","instance-id":"i-1","tcp-flags":"0","type":"IPv4","pkt-srcaddr":"1.72.101.193","pkt-dstaddr":"1.72.63.234"},"meta":{"meta_s3_file":"Log.gz","meta_account_no":"100000674","meta_region":"us-east-1","meta_account_name":"11-2"},"id":"c91cb7f7-02ca-4662-9993-0686f724cb6a"}
