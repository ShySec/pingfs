# PingFS: Living in the Rain
PingFS is a set of python scripts which, in Linux, provide virtual disk storage on the network. Each file is broken up into 64-1024 byte blocks, sent over the wire in an ICMP echo request, and promptly erased from memory. Each time the server sends back your bundle of joy, PingFS recognizes its data and sends another gift [the data]. Download at the bottom of the page. 

## Usage

- mkdir mount_dir/
- python ping_fuse.py mount_dir
- ls mount_dir
- echo cats > mount_dir/reddit
- cat mount_dir/reddit

## Requirements

- Linux
- python 2.7+
- python-fuse
