# !/bin/bash
set -e
echo "### Start to pack Doris: $version ###"
doris_home=`pwd`
version=`grep "^build_version=" gensrc/script/gen_build_version.sh | awk '{print substr($0, 16, length($0) - 16)}'`
dist=doris-$version
brokers=fs_brokers/apache_hdfs_broker/
output_dir=$doris_home/output
cd $doris_home
rm -rf $dist $dist.tar.gz

# add libstdc++.so.6.0.21+ to palo_be's dir for udf runtime dependcy
if [ -z ${LIB_STD_CPP_PATH}];then
  echo "LIB_STD_CPP_PATH is not set, use default path: /usr/lib64/libstdc++.so.6.0.24"
  export LIB_STD_CPP_PATH=/usr/lib64/libstdc++.so.6.0.24
fi
if [ ! -f ${LIB_STD_CPP_PATH} ];then
  echo "${LIB_STD_CPP_PATH} is not exists"
  exit 1
fi
LIBSTDCPP_DEST_PATH=$output_dir/be/lib
if [ ! -d ${LIBSTDCPP_DEST_PATH} ];then
  echo "Build Doris Error!!!"
  exit 1
fi
cp --remove-destination ${LIB_STD_CPP_PATH} ${LIBSTDCPP_DEST_PATH}
if [[ $(md5sum output/be/lib/libstdc++.so.6.0.24 | awk '{split($0, a, " ");print a[1]}') != "d5137627721d3192de4c52be82b1a8d5" ]];then
  echo "libstdc++.so.6's checksum is not match"
  exit 1
fi

cp -r $output_dir $dist
cp -r $brokers/output/apache_hdfs_broker $dist/hdfs_broker
tar -czvf $dist.tar.gz $dist
rm -rf $dist
echo "### Pack Doris: $version Succeed ###"
