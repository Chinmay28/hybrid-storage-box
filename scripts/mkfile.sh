# Sample usage:  bash mkfile <filename> <filesize>

# root@ip-172-31-3-110:~# bash mkfile file1 2M
# 2+0 records in
# 2+0 records out
# 2097152 bytes (2.1 MB, 2.0 MiB) copied, 0.00162618 s, 1.3 GB/s

dd if=/dev/zero iflag=count_bytes count="$2" bs=1M of="$1"; sync
