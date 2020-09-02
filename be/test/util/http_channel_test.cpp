// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/http_channel.h"

#include <gtest/gtest.h>

#include "util/block_compression.h"
#include "util/logging.h"

namespace doris {

class HttpChannelTest : public testing::Test {
public:
    HttpChannelTest() {
        ASSERT_TRUE(get_block_compression_codec(segment_v2::CompressionTypePB::ZLIB, &zlib_codec).ok());
    }

    void check_data_eq(const std::string& output, const std::string& expected) {
        Slice uncompressed_content;
        ASSERT_TRUE(zlib_codec->decompress(Slice(output), &uncompressed_content).ok());
        ASSERT_EQ(expected, uncompressed_content.to_string());
    }

private:
    const BlockCompressionCodec *zlib_codec = nullptr;
};

TEST(HttpChannelTest, CompressContent) {
    ASSERT_FALSE(HttpChannel::compress_content("gzip", "", nullptr));
    ASSERT_FALSE(HttpChannel::compress_content("", "test", nullptr));
    ASSERT_FALSE(HttpChannel::compress_content("Gzip", "", nullptr));

    std::string intput("test_data");
    std::string output;

    ASSERT_TRUE(HttpChannel::compress_content("gzip", intput, &output));
    check_data_eq(output, intput);

    ASSERT_TRUE(HttpChannel::compress_content("123,gzip,321", intput, &output));
    check_data_eq(output, intput);
}

} // namespace doris

int main(int argc, char** argv) {
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

