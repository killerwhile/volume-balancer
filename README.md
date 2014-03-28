DataNode Volumes Rebalancing tool for Apache Hadoop HDFS
===============

This project aims at filling the gap with [HDFS-1312](https://issues.apache.org/jira/browse/HDFS-1312) & family: when a hard drive dies on a Datanode and gets replaced, there is not real way to move blocks from most used hard disks to the newly added -- and thus empty.

[HDFS-1804](https://issues.apache.org/jira/browse/HDFS-1804) is a really good addition that allows the DataNode to choose the least used hard drive, but for new blocks only. Balancing existing blocks is still something missing and thus the main focus of this project.

# Usage

## Using hadoop jar

    $ hadoop jar /var/lib/volume-balancer/lib/volume-balancer-<version>.jar org.apache.hadoop.hdfs.server.datanode.VolumeBalancer [-threshold=0.1] [-concurrency=1]

## Using plain java

    $ java -cp /var/lib/volume-balancer/lib/volume-balancer-<version>.jar:/path/to/hdfs-site.xml/parentDir:$(find $(hadoop classpath | sed -e "s/:/ /g") -maxdepth 0 2>/dev/null | paste -d ":" -s -
) org.apache.hadoop.hdfs.server.datanode.VolumeBalancer [-threshold=0.1] [-concurrency=1]

## With CDH

Cloudera is splitting the configuration files and store in _/etc/hadoop/conf_ only strict minimum. ``dfs.datanode.data.dir`` for instance is not found in _/etc/hadoop/conf/hdfs-site.xml_ so a workaround needs to be put in place. Moreover, once you found the proper _/var/run/cloudera-scm-agent/process_ folder to add in your classpath, you need to skip the _log4j.properties_ file shipped otherwise you'll start a custom proprietary logger. A helper script is given in _src/main/scripts_ folder, which run the balancer in CDH distributions.

    $ /var/lib/volume-balancer/bin/cdh-balance-local-volumes.sh [-threshold=0.1] [-concurrency=1]

# Parameters

## Threshold

Default value: 0.1 (10%)

The script stops when all the drives reach the average disks utilization +/- the threshold.
The smaller the value, the more balanced the drives will be and the longer the process will take.

## Concurrency

Default value: 1 (no concurrency)

Define how many threads are concurrently reading (from different disks) and writing to the least used disk(s).
Advised value is 2, increasing the concurrency does not *always* increase the overall throughput.

# How it works

The script take a random ``subdir*`` leaf (i.e. without other subdir inside) from the most used partition
and move it to a random ``subdir*`` (not exceeding ``DFSConfigKeys.DFS_DATANODE_NUMBLOCKS_KEY``) of the least used partition.
The ``subdir`` keyword comes from ``DataStorage.BLOCK_SUBDIR_PREFIX``

The script is doing pretty good job at keeping the bandwidth of the least used hard drive maxed out using
``FileUtils#moveDirectory(File, File)`` and a dedicated ``j.u.c.ExecutorService`` for the copy. Increasing the
concurrency of the thread performing the copy does not *always* help to improve disk utilization, more particularly
at the target disk. But if you use -concurrency > 1, the script is balancing the read (if possible) amongst several
disks.

Once all disks reach the disks average utilization +/- threshold (can be given as input parameter, by default 0.1)
the script stops. But it can also be **safely stopped** at any time hitting **Crtl+C**: it shuts down properly ensuring **ALL
blocks** of a ``subdir`` are moved, leaving the datadirs in a proper state.

# Monitoring

Appart of the standard ``df -h`` command to monitor disks fulling in, the disk bandwidths can be easily monitored using ``iostat -x 1 -m``

```
$ iostat -x 1 -m
   Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await  svctm  %util
   sdd               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
   sde               0.00 32911.00    0.00  300.00     0.00   149.56  1020.99   138.72  469.81   3.34 100.00
   sdf               0.00    27.00  963.00   50.00   120.54     0.30   244.30     1.37    1.35   0.80  80.60
   sdg               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
   sdh               0.00     0.00  610.00    0.00    76.25     0.00   255.99     1.45    2.37   1.44  88.10
   sdi               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
```

## License

Copyright © 2013-2014 Benoit Perroud

See [LICENSE](LICENSE) for licensing information.

# Contributions

All contributions are welcome: ideas, documentation, code, patches, bug reports, feature requests etc.  And you don't
need to be a programmer to speak up!

If you are new to GitHub please read [Contributing to a project](https://help.github.com/articles/fork-a-repo) for how
to send patches and pull requests to this project.

