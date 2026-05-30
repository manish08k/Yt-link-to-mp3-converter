#!/usr/bin/env bash
set -e
pip install -r requirements.txt
# Install ffmpeg binary
mkdir -p /opt/render/ffmpeg
cd /tmp
wget -q https://github.com/yt-dlp/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-gpl.tar.xz
tar -xf ffmpeg-master-latest-linux64-gpl.tar.xz
cp ffmpeg-master-latest-linux64-gpl/bin/ffmpeg /opt/render/ffmpeg/
cp ffmpeg-master-latest-linux64-gpl/bin/ffprobe /opt/render/ffmpeg/
echo "ffmpeg installed: $(/opt/render/ffmpeg/ffmpeg -version | head -1)"
