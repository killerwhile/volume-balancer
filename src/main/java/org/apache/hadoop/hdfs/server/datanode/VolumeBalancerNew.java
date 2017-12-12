package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.log4j.Logger;

/**
 * Apache HDFS Datanode internal blocks rebalancing script for Hadoop 2.6+
 * 
 * The script take a random subdir (@see {@link DataStorage#BLOCK_SUBDIR_PREFIX}) leaf (i.e. without other subdir
 * inside) from the most used partition and move it to a same subdir of the least used partition
 * 
 * The script is doing pretty good job at keeping the bandwidth of the target volume max'ed out using
 * {@link FileUtils#moveDirectory(File, File)} and a dedicated {@link ExecutorService} for the copy. Increasing the
 * concurrency of the thread performing the copy does not *always* help to improve disks utilization, more particularly
 * at the target disk. But if you use -concurrency > 1, the script is balancing the read (if possible) amongst several
 * disks.
 * 
 * $ iostat -x 1 -m
 *   Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await  svctm  %util
 *   sdd               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
 *   sde               0.00 32911.00    0.00  300.00     0.00   149.56  1020.99   138.72  469.81   3.34 100.00
 *   sdf               0.00    27.00  963.00   50.00   120.54     0.30   244.30     1.37    1.35   0.80  80.60
 *   sdg               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
 *   sdh               0.00     0.00  610.00    0.00    76.25     0.00   255.99     1.45    2.37   1.44  88.10
 *   sdi               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
 * 
 * Once all disks reach the disks average utilization +/- threshold (can be given as input parameter, by default 0.1)
 * the script stops. But it can also be safely stopped at any time hitting Crtl+C: it shuts down properly when ALL
 * blocks of a subdir are moved, leaving the datadirs in a proper state
 * 
 * Usage: java -cp volume-balancer-1.0.0-SNAPSHOT-jar-with-dependencies.jar:/path/to/hdfs-site.conf/parentDir
 * org.apache.hadoop.hdfs.server.datanode.VolumeBalancerNew [-threshold=0.1] [-concurrency=1] [-dirs=128]
 * <p>
 * Attention: u must run this script on datanode by user hdfs, if kerberos enabled, user hdfs must kinit first
 * <p>
 * Disk bandwidth can be easily monitored using $ iostat -x 1 -m
 * 
 * 
 * @author bperroud
 * 
 */
public class VolumeBalancerNew {

    private static final Logger LOG = Logger.getLogger(VolumeBalancerNew.class);

    private static void usage() {
        LOG.info("Available options: \n" + " -threshold=d, default 0.1\n -concurrency=n, default 1\n -dirs=n, default 128\n"
            + VolumeBalancerNew.class.getCanonicalName());
    }

    private static final Random r = new Random();
    private static final int DEFAULT_CONCURRENCY = 1;

    static class Volume implements Comparable<Volume> {
        private final URI uri;
        private final File uriFile;

        Volume(final URI uri) {
            this.uri = uri;
            this.uriFile = new File(this.uri);
        }

        double getUsableSpace() throws IOException {
            return uriFile.getUsableSpace();
        }

        double getTotalSpace() throws IOException {
            return uriFile.getTotalSpace();
        }

        double getPercentAvailableSpace() throws IOException {
            return getUsableSpace() / getTotalSpace();
        }

        @Override
        public String toString() {
            return this.getClass().getName() + "{" + uri + "}";
        }

        @Override
        public int compareTo(Volume arg0) {
            return uri.compareTo(arg0.uri);
        }
    }

    static class SubdirTransfer {
        final File from;
        final File to;
        final Volume volume;

        public SubdirTransfer(final File from, final File to, final Volume volume) {
            this.from = from;
            this.to = to;
            this.volume = volume;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        double threshold = 0.1;
        int concurrency = DEFAULT_CONCURRENCY;
        int dirs = 128;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("-threshold")) {
                String[] split = arg.split("=");
                if (split.length > 1) {
                    threshold = Double.parseDouble(split[1]);
                }
            }
            else if (arg.startsWith("-concurrency")) {
                String[] split = arg.split("=");
                if (split.length > 1) {
                    concurrency = Integer.parseInt(split[1]);
                }
            }
            else if (arg.startsWith("-dirs")) {
                String[] split = arg.split("=");
                if (split.length > 1) {
                    dirs = Integer.parseInt(split[1]);
                }
            }
            else {
                LOG.error("Wrong argument " + arg);
                usage();
                System.exit(2);
            }
        }

        LOG.info("Threshold is " + threshold);

        // Hadoop *always* need a configuration :)
        final HdfsConfiguration conf = new HdfsConfiguration();

        final String blockpoolID = getBlockPoolID(conf);

        LOG.info("BlockPoolId is " + blockpoolID);

        final Collection<URI> dataDirs = getStorageDirs(conf);

        if (dataDirs.size() < 2) {
            LOG.error("Not enough data dirs to rebalance: " + dataDirs);
            return;
        }

        concurrency = Math.min(concurrency, dataDirs.size() - 1);

        LOG.info("Concurrency is " + concurrency);

        final List<Volume> allVolumes = new ArrayList<Volume>(dataDirs.size());
        for (URI dataDir : dataDirs) {
            Volume v = new Volume(dataDir);
            allVolumes.add(v);
        }

        final Set<Volume> volumes = Collections.newSetFromMap(new ConcurrentSkipListMap<Volume, Boolean>());
        final Set<String> movedSubdirs = Collections.newSetFromMap(new ConcurrentSkipListMap<String, Boolean>());
        final int nums = dirs <= 256 ? dataDirs.size() * dirs * 256 : dataDirs.size() * 128 * 256;
        volumes.addAll(allVolumes);

        // Ensure all finalized folders exists
        boolean dataDirError = false;
        for (Volume v : allVolumes) {
            final File f = generateFinalizeDirInVolume(v, blockpoolID);
            if (!f.isDirectory()) {
                if (!f.mkdirs()) {
                    LOG.error("Failed creating " + f + ". Please check configuration and permissions");
                    dataDirError = true;
                }
            }
        }
        if (dataDirError) {
            System.exit(3);
        }
        
        // The actual copy is done in a dedicated thread, polling a blocking queue for new source and target directory
        final ExecutorService copyExecutor = Executors.newFixedThreadPool(concurrency);
        final BlockingQueue<SubdirTransfer> transferQueue = new LinkedBlockingQueue<SubdirTransfer>(concurrency);
        final AtomicBoolean run = new AtomicBoolean(true);
        final CountDownLatch shutdownLatch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new WaitForProperShutdown(shutdownLatch, run));

        for (int i = 0; i < concurrency; i++) {
            copyExecutor.execute(new SubdirCopyRunner(run, transferQueue, volumes));
        }

        // no other runnables accepted for this TP.
        copyExecutor.shutdown();

        boolean balanced = false;
        do {

            double totalPercentAvailable = 0;

            /*
             * Find the least used volume and pick a random subdir folder in that volume
             */
            Volume leastUsedVolume = null;
            for (Volume v : allVolumes) {
                if (leastUsedVolume == null || v.getUsableSpace() > leastUsedVolume.getUsableSpace()) {
                    leastUsedVolume = v;
                }
                totalPercentAvailable += v.getPercentAvailableSpace();
            }
            LOG.debug("leastUsedVolume: " + leastUsedVolume + ", "
                + (int) (leastUsedVolume.getPercentAvailableSpace() * 100) + "% usable");

            totalPercentAvailable = totalPercentAvailable / dataDirs.size();
            LOG.info("total percent available is " + totalPercentAvailable);

            // Check if the volume is balanced (i.e. between totalPercentAvailble +/- threshold)
            if (totalPercentAvailable - threshold < leastUsedVolume.getPercentAvailableSpace()
                && totalPercentAvailable + threshold > leastUsedVolume.getPercentAvailableSpace()) {
                LOG.info("Least used volumes is within the threshold, we can stop.");
                balanced = true;
                break;
            }

            final File leastUsedBlockSubdir = generateFinalizeDirInVolume(leastUsedVolume, blockpoolID);

            /*
             * Find the most used volume and pick a random subdir folder what will be used as a source of move
             */
            Volume mostUsedVolume = null;
            do {
                for (Volume v : volumes) {
                    if (v != leastUsedVolume && (mostUsedVolume == null || v.getUsableSpace() < mostUsedVolume.getUsableSpace())) {
                        mostUsedVolume = v;
                    }
                }
                if (mostUsedVolume == null) {
                    // All the drives are used for a copy. Maybe concurrency might be slightly reduced
                    try {
                        Thread.sleep(100);
                    }
                    catch (InterruptedException e) {
                        break;
                    }
                }
            }
            while (mostUsedVolume == null);

            if (!run.get()) {
                break;
            }

            // Remove it for next iteration
            volumes.remove(mostUsedVolume);

            LOG.debug("mostUsedVolume: " + mostUsedVolume + ", "
                + (int) (mostUsedVolume.getPercentAvailableSpace() * 100) + "% usable");

            File mostUsedBlockSubdir = generateFinalizeDirInVolume(mostUsedVolume, blockpoolID);

            File tmpMostUsedBlockSubdir = null;
            String relativePath = null;
            do {
                // get parent subdir: subdir0 ~ subdir255
                File firstSubdir = getRandomSubdir(mostUsedBlockSubdir);
                if (firstSubdir != null) {
                    // get children subdir: subdir0 ~ subdir255 / subdir0 ~ subdir255
                    tmpMostUsedBlockSubdir = getRandomSubdir(firstSubdir);
                    // if already moved, we should't do it again
                    if (tmpMostUsedBlockSubdir != null && movedSubdirs.add(tmpMostUsedBlockSubdir.getPath())) {
                        relativePath = mostUsedBlockSubdir.toURI().relativize(tmpMostUsedBlockSubdir.toURI()).getPath();
                        mostUsedBlockSubdir = tmpMostUsedBlockSubdir;
                    }
                }
                if (movedSubdirs.size() >= nums) {
                    LOG.info(String.format("Moved subdirs reached our limit %d, we can stop.", nums));
                    balanced = true;
                    break;
                }
            }
            while (tmpMostUsedBlockSubdir == null || relativePath == null);

            if (relativePath == null || relativePath.isEmpty()) {
                break;
            }
            final File finalLeastUsedBlockSubdir = new File(leastUsedBlockSubdir, relativePath);
            LOG.debug(String.format("the dir needs to be moved: %s, the dir can recive blocks: %s",
                mostUsedBlockSubdir, finalLeastUsedBlockSubdir));

            /*
             * Schedule the two subdir for a move.
             */
            final SubdirTransfer st = new SubdirTransfer(mostUsedBlockSubdir, finalLeastUsedBlockSubdir, mostUsedVolume);

            boolean scheduled = false;
            while (run.get() && !(scheduled = transferQueue.offer(st, 1, TimeUnit.SECONDS))) {
                // waiting, while checking if the process is still running
            }
            if (scheduled && run.get()) {
                LOG.info("Scheduled move from " + st.from + " to " + st.to);
            }
        }
        while (run.get() && !balanced);

        run.set(false);

        // Waiting for all copy thread to finish their current move
        copyExecutor.awaitTermination(10, TimeUnit.MINUTES);

        LOG.info(String.format("All dirs we have handled num is %d, default max num is %d", movedSubdirs.size(), nums));
        // TODO: print some reports for your manager

        // Let the shutdown thread finishing
        shutdownLatch.countDown();

    }

    private static File[] findSubdirs(File parent) {
        return parent.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().startsWith(DataStorage.BLOCK_SUBDIR_PREFIX);
            }
        });
    }

    private static File getRandomSubdir(File parent) {

        File[] files = findSubdirs(parent);

        if (files == null || files.length == 0) {
            return null;
        }
        else {
            return files[r.nextInt(files.length)];
        }
    }
    
    private static String getBlockPoolID(Configuration conf) throws IOException {

        final Collection<URI> namenodeURIs = DFSUtil.getNsServiceRpcUris(conf);
        URI nameNodeUri = namenodeURIs.iterator().next();

        final NamenodeProtocol namenode = NameNodeProxies.createProxy(conf, nameNodeUri, NamenodeProtocol.class)
            .getProxy();
        final NamespaceInfo namespaceinfo = namenode.versionRequest();
        return namespaceinfo.getBlockPoolID();
    }

    private static File generateFinalizeDirInVolume(Volume v, String blockpoolID) {
        return new File(new File(v.uri), Storage.STORAGE_DIR_CURRENT + File.separator + blockpoolID + File.separator
            + Storage.STORAGE_DIR_CURRENT + File.separator + DataStorage.STORAGE_DIR_FINALIZED);
    }

    private static class WaitForProperShutdown extends Thread {
        private final CountDownLatch shutdownLatch;
        private final AtomicBoolean run;

        public WaitForProperShutdown(CountDownLatch l, AtomicBoolean b) {
            this.shutdownLatch = l;
            this.run = b;
        }

        @Override
        public void run() {
            LOG.info("Shutdown caught. We'll finish the current move and shutdown.");
            run.set(false);
            try {
                shutdownLatch.await();
            }
            catch (InterruptedException e) {
                // well, we want to shutdown anyway :)
            }
        }
    }

    private static class SubdirCopyRunner implements Runnable {

        private final BlockingQueue<SubdirTransfer> transferQueue;
        private final AtomicBoolean run;
        private final Set<Volume> volumes;

        public SubdirCopyRunner(AtomicBoolean b, BlockingQueue<SubdirTransfer> bq, Set<Volume> v) {
            this.transferQueue = bq;
            this.run = b;
            this.volumes = v;
        }

        @Override
        public void run() {

            while (run.get()) {
                SubdirTransfer st = null;
                try {
                    st = transferQueue.poll(1, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                }

                if (st != null) {

                    long start = System.currentTimeMillis();

                    try {
                        File[] files = FileUtils.listFiles(st.from, FileFilterUtils.trueFileFilter(), null).toArray(new File[0]);
                        // sort for block and block meta together
                        Arrays.sort(files);
                        for (File move : files) {
                            FileUtils.moveFileToDirectory(move, st.to, true);
                            LOG.debug("move file " + move.getPath());
                        }
                        LOG.info("move all files in " + st.from + " to " + st.to + " took " + (System.currentTimeMillis() - start)
                            + "ms");
                    }
                    catch (java.io.FileNotFoundException e) {
                        // Corner case when the random source folder has been picked by the previous run
                        // skipping it is safe
                        LOG.warn(st.to + " does not exist, skipping this one.");
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        run.set(false);
                    }
                    finally {
                        volumes.add(st.volume);
                    }
                }
            }

            LOG.info(this.getClass().getName() + " shut down properly.");
        }
    }

    static Collection<URI> getStorageDirs(Configuration conf) {
        Collection<String> dirNames = conf.getTrimmedStringCollection(DFS_DATANODE_DATA_DIR_KEY);
        return Util.stringCollectionAsURIs(dirNames);
    }
}