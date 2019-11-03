#!/usr/bin/env bash
cd src/main/java/com/pingcap/proto/

echo "generate binlog code..."
GOGO_ROOT=${GOPATH}/src/github.com/gogo/protobuf
protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --java_out=. proto binlog.proto
echo $pwd
protoc -I .proto --java_out=. proto/*.proto

