DataNode Volumes Rebalancing tool for Apache Hadoop HDFS
===============

This project aims at filling the gap with [HDFS-1312](https://issues.apache.org/jira/browse/HDFS-1312) & family: when a hard drive dies on a Datanode and gets replaced, there is not real way to move blocks from most used hard disks to the newly added -- and thus empty.

[HDFS-1804](https://issues.apache.org/jira/browse/HDFS-1804) allows the DataNode to choose the least used hard drive, but for new blocks only.

# Usage

    java -cp volume-balancer-<version>-jar-with-dependencies.jar:/path/to/hdfs-site.conf/parentDir org.apache.hadoop.hdfs.server.datanode.VolumeBalancer [-threshold=0.1]
        * the ``threshold`` parameter ensure the newly added disk fills up until the disks average +/- the threshold. By default it's 0.1

# How it works

The script take a random ``subdir*`` leaf (i.e. without other subdir inside) from the most used partition
and move it to a random ``subdir*`` (not exceeding ``DFSConfigKeys.DFS_DATANODE_NUMBLOCKS_KEY``) of the least used partition.
The ``subdir`` keyword comes from ``DataStorage.BLOCK_SUBDIR_PREFIX``

The script is doing pretty good job at keeping the bandwidth of the least used hard drive maxed out using
``FileUtils#moveDirectory(File, File)`` and *one* dedicated ``j.u.c.ExecutorService`` for the copy. Increasing the
concurrency of the thread performing the copy does not help at all do a better utilization of the target disk
throughput.

Once all disks reach the disks average utilization +/- threshold (can be given as input parameter, by default 0.1)
the script stops. But it can also be **safely stopped** at any time hitting **Crtl+C**: it shuts down properly ensuring **ALL
blocks** of a ``subdir`` are moved, leaving the datadirs in a proper state.

# Monitoring

Appart of the standard ``df -h`` command to monitor disks fulling in, the disk bandwidths can be easily monitored using ``iostat -x 1 -m``

