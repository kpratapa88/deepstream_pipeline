# Use NVIDIA DeepStream 7.0 as base image
FROM nvcr.io/nvidia/deepstream:7.0-triton-multiarch

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHON_VERSION=3.10
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/nvidia/deepstream/deepstream/lib

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    python3-setuptools \
    python3-gi \
    python3-gi-cairo \
    libgirepository1.0-dev \
    libcairo2-dev \
    python3-pybind11 \
    pybind11-dev \
    python3-gst-1.0 \
    python3-numpy \
    python3-opencv \
    libgstrtspserver-1.0-0 \
    libgstrtspserver-1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    libgstreamer1.0-dev \
    libglib2.0-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libyaml-cpp-dev \
    libyaml-dev \
    libtool \
    autoconf \
    automake \
    g++ \
    git \
    wget \
    cmake \
    build-essential \
    pkg-config \
    ffmpeg \
    libavcodec-dev \
    libavformat-dev \
    libavutil-dev \
    libswscale-dev \
    libavfilter-dev \
    libavdevice-dev \
    && rm -rf /var/lib/apt/lists/*

# Install DeepStream Python bindings (pyds)
WORKDIR /opt/nvidia/deepstream/deepstream/sources
# Clone a specific tag (v1.1.11) compatible with DeepStream 7.0 (Python 3.10)
RUN git clone -b v1.1.11 https://github.com/NVIDIA-AI-IOT/deepstream_python_apps.git

WORKDIR /opt/nvidia/deepstream/deepstream/sources/deepstream_python_apps
RUN git submodule update --init --recursive

# Build gst-python if needed
RUN if [ -d "3rdparty/gst-python" ]; then \
        cd 3rdparty/gst-python/ && \
        ./autogen.sh PYTHON=python3 && \
        make && \
        make install; \
    fi

# Build pyds
WORKDIR /opt/nvidia/deepstream/deepstream/sources/deepstream_python_apps/bindings
RUN mkdir build && \
    cd build && \
    cmake .. -DPYTHON_MAJOR_VERSION=3 -DPYTHON_MINOR_VERSION=10 && \
    make && \
    pip3 install --ignore-installed --no-deps ./pyds-*.whl

# Install Python packages
RUN pip3 install --upgrade pip
RUN pip3 install --ignore-installed \
    "numpy<2.0" \
    ultralytics \
    onnx \
    onnxruntime-gpu \
    pyyaml \
    requests \
    flask \
    pydantic \
    psutil \
    matplotlib

# Set working directory
WORKDIR /app

# Copy application files
COPY . /app

# Expose ports (for RTSP/Webhooks if needed)
EXPOSE 8554 5000

# Set entrypoint
CMD ["python3", "pipeline.py"]
