package org.apache.hadoop.hdfs.tools;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.CompressUtils;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.Decompressor;

public class BlockDecompressorStream extends InputStream {
  
  private static final Log LOG = LogFactory.getLog(BlockDecompressorStream.class);
  
  // Compressed data stream.
  private InputStream cdata;
  
  // Compressed data index stream.
  private InputStream iin;
  
  // Decompressor of compressed data.
  private Decompressor decompressor;
  
  // Chunk size of compressed data.
  private int chunkSize;
  
  // Length of original data.
  private long srcLen;
  
  // Next position for
  private int chunks;
  
  // Next position for read.
  private long curPos;
  
  // Current cdata position.
  private long cPos;
  
  // Current chunk, the last decompressed chunk, negative for no chunk has been decompressed yet.
  private int curChunk = -1;
  
  // Decompressed data of current chunk.
  private byte[] dbuf;
  
  // Compressed data of current chunk.
  private byte[] cbuf;
  
  // Length of chunks
  private int[] clen;
  
  private boolean closed;
  
  public BlockDecompressorStream(InputStream data, InputStream iin, long srcLen, Decompressor decompressor) throws IOException {
    this.cdata = data;
    this.iin = iin;
    
    DataInputStream idx = new DataInputStream(iin); 
    chunkSize = idx.readInt();
    dbuf = new byte[chunkSize];

    this.srcLen = srcLen;
    this.decompressor = decompressor;
    
    chunks = (int) (Math.ceil(1.0 * srcLen / chunkSize));
    clen = new int [chunks];
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got ChunkSize:" + chunkSize + ", chunks:" + chunks + ", srcLen:" + srcLen);
    }
    
    int largestChunk = clen[0];
    int prevChunkEnd = 0;
    for (int i = 0; i < chunks; i++) {
      int end = idx.readInt();
      clen[i] = end - prevChunkEnd;
      prevChunkEnd = end;
      largestChunk = Math.max(largestChunk, clen[i]);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Chunk len[" + i + "]=" + clen[0]);
      }
    }
    cbuf = new byte[largestChunk];
  }

  byte[] oneByte = new byte[1];
  @Override
  public int read() throws IOException {
    checkStream();
    return (read(oneByte, 0, oneByte.length) == -1) ? -1 : (oneByte[0] & 0xff);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Read off:" + off + ", len:" + len + ", curPos:" + curPos + ", curChunk:" + curChunk + ", srcLen:" + srcLen);
    }
    checkStream();
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    } else if (curPos == srcLen) {
      return -1;
    } else if (len == 0) {
      return 0;
    }
    
    long oldPos = curPos;
    
    int rdOff = off;
    int left = len;
    while (curPos < srcLen) {
      int rd = readLocal(b, rdOff, left);
      curPos += rd;
      rdOff += rd;
      left -= rd;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Read " + rd + " bytes from local buffer, curPos:" + curPos + ", rdOff:" + rdOff + ", left:" + left);
      }
      if (left == 0) {
        break;
      } else {
        if (!nextChunk()) {
          // No next chunk and local buffer is empty.
          break;
        }
      }
    }
    return (int) (curPos - oldPos);
  }
  
  @Override
  public int available() throws IOException {
    checkStream();
    return (int) Math.max(Integer.MAX_VALUE, srcLen - curPos);
  }

  @Override
  public void close() throws IOException {
    if (cdata != null) {
      cdata.close();
    }
    if (iin != null) {
      iin.close();
    }
    decompressor.end();
    closed = true;
  }

  /*
   * Read data from local buffer.
   */
  private int readLocal(byte[] b, int off, int len) {
    int localLeft = leftInBuf();
    int rd = Math.min(len, localLeft);
    int pos = posInBuf();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Read local: off:" + off + ", posInBuf:" + pos + ", rd=min(left:" + localLeft + ", len:" + len + "):" + rd);
    }
    System.arraycopy(dbuf, pos, b, off, rd);
    return rd;
  }

  @Override
  public long skip(long n) throws IOException {
    checkStream();
    if (n < 0) {
      return 0;
    }
    long oldPos = curPos;
    int oldChunk = curChunk;
    long targetPos = Math.min(srcLen, curPos + n);
    int targetChunk = (int) (targetPos / chunkSize);
    
    if (targetChunk > oldChunk) {
      curPos = chunkSize * targetChunk;
      curChunk = targetChunk - 1;
      long skip = 0;
      for (int chunk = oldChunk + 1; chunk < targetChunk; chunk++) {
        skip += clen[chunk];
      }
      if (skip > 0) {
        IOUtils.skipFully(cdata, skip);
        cPos += skip;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Jump to chunk:" + curChunk);
      }
      decompressor.reset();
    }
    int localSkip = (int) (targetPos - curPos);
    if (localSkip > 0 && read(dbuf, 0, localSkip) != localSkip) {
      throw new EOFException("Skip failed:" + localSkip);
    }

    long skip = curPos - oldPos;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Skip " + skip + " bytes, n:" + n);
    }
    return skip;
  }

  private boolean nextChunk() throws IOException {
    if (curChunk + 1 >= chunks) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No chunk left, curChunk:" + curChunk + ", chunks:" + chunks);
      }
      return false;
    }
    
    curChunk++;

    int n = BlockCompressor.fill(cbuf, clen[curChunk], cdata);
    if (n != clen[curChunk]) {
      throw new EOFException("Unexpected end of input stream");
    } else {
      cPos += n;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Next chunk: " + curChunk + "@" + chunks + ", Add new data: " + n);
    }
    decompressor.reset();
    decompressor.setInput(cbuf, 0, n);
    int len = decompress(dbuf, 0, dbuf.length);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Decompressed " + len + " bytes data.");
    }
    return true;
  }
  
  private int decompress(byte[] b, int off, int len) throws IOException {
    int n = 0;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Decompress: b:" + b.length + ", off:" + off + ", len:" + len);
    }
    while ((n = decompressor.decompress(b, off, len)) == 0) {
      if (decompressor.finished()) {
        break;
      }
    }
    return n;
  }
  
  /*
   * Next position for read in local buffer. 
   */
  int posInBuf() {
    if (curChunk < 0) {
      return 0;
    }
    int pos = (int) (curPos - chunkSize * curChunk);
    if (pos < 0) throw new IllegalStateException("curPos:" + curPos + ", curChunk:" + curChunk);
    return pos;
  }
  
  /*
   * Bytes left in local buffer.
   */
  int leftInBuf() {
    if (curChunk < 0) {
      return 0;
    } else {
      int curChunkSize = (curChunk == chunks - 1) ? (int) (srcLen - chunkSize * curChunk) : chunkSize;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Current chunk size:" + curChunkSize + ", srcLen:" + srcLen);
      }
      return curChunkSize - posInBuf();
    }
  }
  
  void checkStream() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
  }
  
  public static void main(String[] arg) throws IOException{
     if(arg.length != 5 && arg.length != 4 && arg.length != 3){
        System.out.println("BlockDecompressorStream <block file path> <index file path> <output path> [Thread num, default = 1] [offset]");
        return;
     }
     int num = 1;
     long offset = 0;
     if(arg.length == 4){
       num = Integer.parseInt(arg[3]);
     }
     if(arg.length == 5)
       offset = Long.parseLong(arg[4]);
     List<Thread> l = new ArrayList<Thread>();
     for(int i = 0; i < num;i++){
       Thread t = new Thread(new DecompressThread(arg[0], arg[1], arg[2] + "/blk_" + i, offset)); 
       l.add(t);
       t.start();
     }
     for(Thread t : l){
       try{
         t.join();
       }catch(InterruptedException e){}
     }
  }
  
  static class DecompressThread implements Runnable{
    String cdata, idx, name;
    long offset;
    
    DecompressThread(String cdata, String idx, String name, long offset){
      this.cdata = cdata;
      this.idx = idx;
      this.name = name;
      this.offset = offset;
    }
    
    
  @Override
  public void run() {
    for(int i = 0; i < 1; i++){
    try{
       File blockFile = new File(cdata);
       Decompressor decomp = CompressUtils.getDecompressor(blockFile.getName());
       if(decomp == null){
         DataNode.LOG.warn("can not get codec of block : " + blockFile.getAbsolutePath());
         throw new IOException("can not get codec of block : " + blockFile.getAbsolutePath());
       }
        
       long blkLenBefCompress = FSDataset.getLengthBefCompress(blockFile.getName());
       FileInputStream cdata = new FileInputStream(blockFile);
       FileInputStream iis = new FileInputStream(new File(idx));
       FileOutputStream fos = new FileOutputStream(new File(name));
       
       BlockDecompressorStream scis = new BlockDecompressorStream(cdata, iis, blkLenBefCompress, decomp);
       scis.skip(offset);
       byte[] buf = new byte[1024 * 1024];
       int len = 0;
       try{
          while((len = scis.read(buf)) > 0){
            fos.write(buf, 0, len);
          }
       }catch(IOException e){
         LOG.warn("", e);
       }finally{
        if(iis != null)
          iis.close();
        if(fos != null)
          fos.close();
        if(cdata != null)
          cdata.close();
       }
    }catch(IOException e){
      e.printStackTrace();
    }
    }
  }
  }
}