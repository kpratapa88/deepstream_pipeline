# ── Base image ────────────────────────────────────────────────────────────────
# DeepStream 7.0 includes: TensorRT 8.6, CUDA 12.2, GStreamer 1.20, nvinfer
FROM nvcr.io/nvidia/deepstream:7.0-triton-multiarch

# ── Environment ───────────────────────────────────────────────────────────────
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHON_VERSION=3.10
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/nvidia/deepstream/deepstream/lib
ENV GST_DEBUG=2

# ── System dependencies ───────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Python build tools
    python3-pip \
    python3-dev \
    python3-setuptools \
    python3-wheel \
    # GObject / GStreamer Python bindings
    python3-gi \
    python3-gi-cairo \
    python3-gst-1.0 \
    libgirepository1.0-dev \
    libcairo2-dev \
    # GStreamer plugins (required for display, encoding, RTSP)
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    libgstrtspserver-1.0-0 \
    libgstrtspserver-1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    libgstreamer1.0-dev \
    # Build tools (for pyds bindings)
    python3-pybind11 \
    pybind11-dev \
    cmake \
    build-essential \
    g++ \
    pkg-config \
    libtool \
    autoconf \
    automake \
    git \
    wget \
    # System libraries
    libglib2.0-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libyaml-cpp-dev \
    libyaml-dev \
    # FFmpeg (for video decode/encode, cv2, ultralytics export)
    ffmpeg \
    libavcodec-dev \
    libavformat-dev \
    libavutil-dev \
    libswscale-dev \
    libavfilter-dev \
    libavdevice-dev \
    && rm -rf /var/lib/apt/lists/*

# ── DeepStream Python bindings (pyds) ─────────────────────────────────────────
# v1.1.11 is compatible with DeepStream 7.0 + Python 3.10
WORKDIR /opt/nvidia/deepstream/deepstream/sources
RUN git clone -b v1.1.11 --depth 1 \
    https://github.com/NVIDIA-AI-IOT/deepstream_python_apps.git

WORKDIR /opt/nvidia/deepstream/deepstream/sources/deepstream_python_apps
RUN git submodule update --init --recursive

# Build gst-python (GStreamer Python overrides)
RUN if [ -d "3rdparty/gst-python" ]; then \
        cd 3rdparty/gst-python/ && \
        ./autogen.sh PYTHON=python3 && \
        make -j$(nproc) && \
        make install; \
    fi

# Build and install pyds wheel
WORKDIR /opt/nvidia/deepstream/deepstream/sources/deepstream_python_apps/bindings
RUN mkdir -p build && cd build && \
    cmake .. -DPYTHON_MAJOR_VERSION=3 -DPYTHON_MINOR_VERSION=10 && \
    make -j$(nproc) && \
    pip3 install --no-deps ./pyds-*.whl

# ── Python dependencies ───────────────────────────────────────────────────────
RUN pip3 install --upgrade pip

# Core numerical / ML
RUN pip3 install \
    "numpy<2.0" \
    "Pillow>=9.0"

# ONNX export + inference
RUN pip3 install \
    onnx \
    onnxruntime \
    onnxslim

# Ultralytics (model export: PT → ONNX)
RUN pip3 install \
    ultralytics

# GPU monitoring (replaces deprecated pynvml)
RUN pip3 install \
    pynvml

# System monitoring
RUN pip3 install \
    psutil

# Utilities
RUN pip3 install \
    pyyaml \
    requests

# ── Working directory & application ──────────────────────────────────────────
WORKDIR /app
COPY . /app

# ── Ports (RTSP output / webhook) ────────────────────────────────────────────
EXPOSE 8554 5000

# ── Entry point ───────────────────────────────────────────────────────────────
CMD ["python3", "main.py", "--streams", "4"]
