# macos本地测试失败，故使用docker进行配置
FROM ubuntu:20.04
CMD bash
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get -y install build-essential && \
    cd minisql && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make -j