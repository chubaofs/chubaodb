# docker build -t ansj/chubaodb_base:1.6.3 .
FROM centos:7

RUN yum update -y
RUN yum install -y wget gcc gcc-c++ make automake git blas-devel lapack-devel which cmake clang
# for great wall
RUN export RUSTUP_DIST_SERVER=https://mirrors.ustc.edu.cn/rust-static 
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN source $HOME/.cargo/env
RUN git clone https://github.com/facebookresearch/faiss.git
RUN cd faiss/ && git checkout v1.6.3
RUN cd faiss/ && ./configure --without-cuda && make install