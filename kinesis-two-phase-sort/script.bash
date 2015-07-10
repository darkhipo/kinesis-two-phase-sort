grep -io "Time: [0-9]*" kinesis-read.log | cut --delimiter=' ' -f 2 | sort -M > read-times-sort
