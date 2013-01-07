package org.apache.hadoop.hdfs.tools;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CompressUtils;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.protocol.BlockCompressCommand;
import org.apache.hadoop.hdfs.server.protocol.InternalDatanodeProtocol;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.quicklz.QuickLzCompressor;
import org.apache.hadoop.io.compress.quicklz.QuickLzDecompressor;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BlockCompressor implements Tool {
  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }
  private static Log LOG = LogFactory.getLog(BlockCompressor.class);

  public static final int QuickLzTYPE = 0;

  public static final int LzoTYPE = 1;
  
  public static final int GzipTYPE = 2;

  private static final float MIN_COMPRESS_RATE = 1.2f;

  // Block small than MIN_DATA_SIZE is impossible to compress, maybe.
  private static final int MIN_DATA_SIZE = 1024;
  
  private static final int BUF_SIZE = 8192;

  public static final int CHUNK_SIZE = 1024 * 1024;

  private Configuration conf;
  
  private int chunkSize = CHUNK_SIZE;
  
  private float minCompressRate = MIN_COMPRESS_RATE;

  private Compressor compressor;
  
  private Decompressor decompressor;

  private InternalDatanodeProtocol datanode;

  private TreeSet<Long> ignoredBlocks;

  private TreeSet<Long> compressedBlocks;
  
  private long sleepIntervel;
  
  private int min_data_size = MIN_DATA_SIZE;
  
  private int compressRetryTimes, type = 0, threadnum = 1;
  
  public static enum CompressStatus {
    OK,
    FAILED,
    IGNORE
  }
  
  public BlockCompressor() {
    compressor = new QuickLzCompressor(CHUNK_SIZE);
    decompressor = new QuickLzDecompressor(CHUNK_SIZE);
    ignoredBlocks = new TreeSet<Long>();
    compressedBlocks = new TreeSet<Long>();
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    chunkSize = conf.getInt("dfs.block.compress.chunk.size", CHUNK_SIZE);
    minCompressRate = conf.getFloat("dfs.block.compress.rate.min", MIN_COMPRESS_RATE);
    sleepIntervel = conf.getLong("block.compressor.sleep.interval", 1000);
    compressRetryTimes = conf.getInt("block.compressor.retry.times", 3);
    min_data_size = conf.getInt("block.compressor.min.size", MIN_DATA_SIZE);
    type = conf.getInt("block.compressor.codec.type", 0);
    threadnum = conf.getInt("block.compressor.thread.num", 1);
    Decompressor decompress = CompressUtils.getDecompressor(type);
    if(decompress == null){
      throw new IllegalArgumentException("can not support this codec type. block.compressor.codec.type" + type);
    }
    this.decompressor = decompress;
    Compressor compress = CompressUtils.getCompressor(type);
    if(compress == null){
      throw new IllegalArgumentException("can not support this codec type. block.compressor.codec.type" + type);
    }
    this.compressor = compress;
    
    cleanup(conf);  // cleanup tmp file
  }

  public Configuration getConf() {
    return conf;
  }

  private long[] convertSet2Array(TreeSet<Long> set){
    if(set == null)
      return new long[0];
    long [] buf = new long[set.size()];
    int i = 0;
    for(Long l : set){
      buf[i++] = l;
    }
    return buf;
  }
  public int run(String[] args) throws Exception {
    init();
    while (true) {
      try{
        long[] ign = convertSet2Array(ignoredBlocks);
        long[] complete = convertSet2Array(compressedBlocks);
        BlockCompressCommand task = datanode.getTask(ign , complete, 0);
        ignoredBlocks.clear();
        compressedBlocks.clear();
        List<String> pathlist = task.getPathList();
        if(pathlist != null && pathlist.size() != 0){
          LOG.info("get task from datanode . task num : " + pathlist.size());
          List<Thread> list = new ArrayList<Thread>(threadnum);
          for (String blockFile : pathlist) {
            compress(blockFile);
          }
        }
      } catch (Throwable e) {
        LOG.info("", e);
        //:~
      } finally{
        Thread.sleep(sleepIntervel);
      }
    }
  }

  public void init() throws IOException {
    datanode = createDatanode(conf);
  }
  
  public void shutdown() throws IOException {
    RPC.stopProxy(datanode);
  }

  /*
   * Build a InternalDatanodeProtocol connection to the datanode and set up the retry
   * policy
   */
  private static InternalDatanodeProtocol createDatanode(Configuration conf)
      throws IOException {
    @SuppressWarnings("deprecation")
    InetSocketAddress socAddr = NetUtils.createSocketAddr(conf.get("dfs.datanode.ipc.address"));
    RetryPolicy timeoutPolicy = RetryPolicies.exponentialBackoffRetry(5, 200,
        TimeUnit.MILLISECONDS);
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap = new HashMap<Class<? extends Exception>, RetryPolicy>();
    RetryPolicy methodPolicy = RetryPolicies.retryByException(timeoutPolicy,
        exceptionToPolicyMap);
    Map<String, RetryPolicy> methodNameToPolicyMap = new HashMap<String, RetryPolicy>();
    methodNameToPolicyMap.put("getTask", methodPolicy);

    UserGroupInformation ugi;
    try {
      ugi = UnixUserGroupInformation.login(conf);
    } catch (javax.security.auth.login.LoginException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }

    return (InternalDatanodeProtocol) RetryProxy.create(
        InternalDatanodeProtocol.class, RPC.getProxy(InternalDatanodeProtocol.class,
            InternalDatanodeProtocol.versionID, socAddr, ugi, conf,
            NetUtils.getDefaultSocketFactory(conf)), methodNameToPolicyMap);
  }

  public long[] getBlockIds(Collection<Block> blocks) {
    long[] ids = new long[blocks.size()];
    int idx = 0;
    for (Block block : blocks) {
      ids[idx++] = block.getBlockId();
    }
    return ids;
  }

  public int getChunkSize() {
    return chunkSize;
  }

  public CompressStatus compress(String blockFilePath) {
    File blockFile = new File(blockFilePath);

    int retry = compressRetryTimes;
    CompressStatus stat = null;
    while(retry > 0) {
      try {
        CompressStatus ret = compress(blockFile);
        return ret;
      } catch (IOException e) {
        retry--;
      }
    }
    return stat;
  }

  private File getTmpIdxFile(File parent, long blockId) {
    return new File(parent, "blk_" + blockId + ".idx.tmp");
  }

  private File getTmpDataFile(File parent, long blockId) {
    return new File(parent, "blk_" + blockId + ".cdata.tmp");
  }
  
  public static File getIdxFile(File parent, long blockId) {
    return new File(parent, "blk_" + blockId + ".idx");
  }
  
  public static File getCompressDataFile(File parent, long blockId, long oldBlockSize, int compressType) {
    return new File(parent, "blk_" + blockId + "_" + oldBlockSize + "_" + compressType + ".cdata");
  }

  public CompressStatus compress(File dataFile) throws IOException {
    long blockId = CompressUtils.filename2id(dataFile.getName());
    LOG.info("try to compress file " + dataFile.getAbsolutePath());
    if (dataFile.length() < min_data_size) {
      LOG.info("ignore " + dataFile.getName() + ", because file len < MIN_DATA_SIZE . file len : "
          + dataFile.length() + " , MIN len : " + min_data_size);
      return ignore(blockId);
    }
    
    File parent = dataFile.getParentFile();
    File tmpBlockFile = getTmpDataFile(parent, blockId);
    File tmpIdxFile = getTmpIdxFile(parent, blockId);
    
    BufferedInputStream in = null;
    BufferedOutputStream out = null;
    DataOutputStream idxOut = null;
    try {
      in = new BufferedInputStream(new FileInputStream(dataFile));
      out = new BufferedOutputStream(new FileOutputStream(tmpBlockFile));
      idxOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmpIdxFile)));
      byte[] rdBuf = new byte[chunkSize];
      byte[] wtBuf = new byte[BUF_SIZE];

      int srcRead = 0;
      int destIdx = 0;

      idxOut.writeInt(chunkSize);
      
      int rd = 0;
      while ((rd = fill(rdBuf, rdBuf.length, in)) > 0) {
        srcRead += rd;
        compressor.reset();
        compressor.setInput(rdBuf, 0, rd);
        compressor.finish();
        
        while (!compressor.finished()) {
          int wt = compressor.compress(wtBuf, 0, wtBuf.length);
          out.write(wtBuf, 0, wt);
          destIdx += wt;
        }
        double compressRate =  (double) srcRead / (double) destIdx;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Write idx: " + srcRead + "->" + destIdx);
        }
        if (compressRate > minCompressRate) {
          idxOut.writeInt(destIdx);
          out.flush();
          idxOut.flush();
        } else {
          LOG.info("ignore " + dataFile.getName() + ", because file compress rate less than min compress rate . compress rate : " 
                + compressRate + " , min compress rate : " + minCompressRate);
          cleanup(tmpBlockFile, tmpIdxFile);
          return ignore(blockId);
        }
      }
      // test compressed file
      InputStream rin = null;
      InputStream sin = null;
      try {
        rin = decompress(tmpBlockFile, tmpIdxFile, srcRead);
        sin = new BufferedInputStream(new FileInputStream(dataFile));
        int lhs;
        int rhs;
        do {
          lhs = rin.read();
          rhs = sin.read();
          if (rhs != lhs) throw new IOException("Uncompress failed.");
        } while (lhs != -1);
      } catch (Exception e) {
        LOG.warn("error in decompress block : " + blockId, e);
        CompressStatus status = ignore(blockId);
        LOG.info("clean up tmp file. " + (tmpBlockFile.delete() && tmpIdxFile.delete()));
        return status;
      } finally {
        if (rin != null) rin.close();
        if (sin != null) sin.close();
      }
      
      
      if (tmpBlockFile.renameTo(getCompressDataFile(parent, blockId, srcRead, type))
          && tmpIdxFile.renameTo(getIdxFile(parent, blockId))) {
        LOG.info("complete compress file : " + dataFile.getName());
        CompressStatus status =  finish(blockId);
        return status;
      } else {
        throw new IOException("Rename tmp files failed.");
      }
    } catch (IOException e) {
      cleanup(tmpBlockFile, tmpIdxFile);
      throw e;
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          LOG.warn("Close source block file failed ( " + blockId + " ).", e);
        }
      }
      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
          LOG.warn("Close target block file failed ( " + blockId + " ).", e);
        }
      }
      if (idxOut != null) {
        try {
          out.close();
        } catch (IOException e) {
          LOG.warn("Close index file failed ( " + blockId + " ).", e);
        }
      }
    }
  }
  
  public CompressStatus finish(long blockId) {
    compressedBlocks.add(blockId);
    return CompressStatus.OK;
  }
  
  public CompressStatus ignore(long blockId) {
    ignoredBlocks.add(blockId);
    return CompressStatus.IGNORE;
  }
  
  public void cleanup(File tmpDataFile, File tmpIdxFile) {
    if (!tmpDataFile.delete()) {
      LOG.warn("Cleanup tmp file failed: " + tmpDataFile);
    }
    if (!tmpIdxFile.delete()) {
      LOG.warn("Cleanup tmp file failed: " + tmpIdxFile);
    }
  }
  
  static int fill(byte[] buf, int len, InputStream in) throws IOException {
    int rd = 0;
    int idx = 0;
    while (idx < len && (rd = in.read(buf, 0, len)) != -1) {
      idx += rd;
    }
    return idx;
  }

  void cleanup(Configuration conf){
     String[] strs = conf.getStrings("dfs.data.dir");
     for(String vol : strs){
       File pdir = new File(vol, Storage.STORAGE_DIR_CURRENT);
       deleteRecurseTmp(pdir);
     }
  }
  static void deleteRecurseTmp(File file){
    if (file.isFile()) {
      if(file.getName().endsWith(".tmp"))
        file.delete();
    }else {
      File[] files = file.listFiles();
      if(files == null || files.length == 0)
        return;
      for(File f : files){
        deleteRecurseTmp(f);
      }
    }
  }
  
  public long getSrcLength(String filename) throws IllegalArgumentException {
    String[] sa = filename.split("_");
    if (sa.length == 4) {
      return Long.parseLong(sa[2]);
    }
    throw new IllegalArgumentException();
  }
      
  public InputStream decompress(File cdata, File idx) throws FileNotFoundException, IllegalArgumentException, IOException {
    return decompress(cdata, idx, getSrcLength(cdata.getName()));
  }
  
  public InputStream decompress(File cdata, File idx, long srcLength) throws FileNotFoundException, IllegalArgumentException, IOException {
    if(!cdata.exists() || !idx.exists()){
      throw new IOException("cdata file or index file can not be found.cdata path " +
          cdata.getAbsolutePath() + " cdata is exits ? " + (cdata.exists()) + " index is exist? " + (idx.exists()));
    }
    return new BlockDecompressorStream(new BufferedInputStream(new FileInputStream(cdata)), new BufferedInputStream(new FileInputStream(idx)), srcLength, CompressUtils.getDecompressor(type));
  }
  
  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("hadoop.native.lib", false);
    BlockCompressor bc = new BlockCompressor();
    bc.setConf(conf);
    try {
      System.exit(ToolRunner.run(null, bc, args));
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

}
