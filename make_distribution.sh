# !/bin/bash
set -e
doris_home=`pwd`
version=`grep "^build_version=" gensrc/script/gen_build_version.sh | awk '{print substr($0, 16, length($0) - 16)}'`
dist=doris-$version
cd $doris_home
echo "### Compile and Pack Doris: $version ###"
rm -rf output
echo "### Start to Build for Doris Module"
bash build.sh --be --fe --clean
brokers=fs_brokers/apache_hdfs_broker/
rm -rf $brokers/output
echo "### Start to Build for Hdfs Broker Plugin ###"
cd $brokers
bash build.sh
cd -
bash ./pack.sh

exit 0
