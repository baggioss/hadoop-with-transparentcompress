package org.apache.hadoop.hdfs.protocol;

import java.io.File;
import java.io.FileFilter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.tools.BlockCompressor;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.quicklz.QuickLzCompressor;
import org.apache.hadoop.io.compress.quicklz.QuickLzDecompressor;

public class CompressUtils {
  public static final Log LOG = LogFactory.getLog(CompressUtils.class);
  public static boolean isCompressBlockFilename(File f) {
    String name = f.getName();
    if ( name.startsWith( "blk_") && name.endsWith(FSConstants.CDATA_EXTENSION )) {
      return true;
    } else {
      return false;
    }
  }
  public static long filename2id(String name) {
    if(name.endsWith(FSConstants.CDATA_EXTENSION)){
      String[] vals = name.split("_");
      if (vals.length != 4) {     // blk, blkid, genstamp, checksum len.cmeta
        return -1;
      }
      return Long.valueOf(vals[1]);
    }else{
      return Long.parseLong(name.substring("blk_".length()));
    }
  }
  
  
  public static Compressor getCompressor(int type){
    return new QuickLzCompressor(BlockCompressor.CHUNK_SIZE);
  }
  
  public static Decompressor getDecompressor(int type){
    return new QuickLzDecompressor(BlockCompressor.CHUNK_SIZE);
  }
  public static Decompressor getDecompressor(String name){
      int type = getDecompressorType(name);
      if(type < 0)
        return null;
      return getDecompressor(type);
  }
  
  public static int getDecompressorType(String name){
    if(!name.endsWith(FSConstants.CDATA_EXTENSION)){
      return -1;
    }else{
      String[] strs = name.split("_");
      if(strs == null || strs.length != 4){
        return -1;
      }
      String[] names = strs[3].split(FSConstants.CDATA_EXTENSION);
      if(names == null || names.length != 1){
        return -1;
      }
      int type = Integer.parseInt(names[0]);
      return type;
    }
  }
  
    
    public static long getGenerationStampFromMeta(File metaFile){
      String name = metaFile.getName();
      String[] parts = name.split("_");
      if(parts == null || parts.length != 3){
        return -1;
      }
      String s = parts[2].substring(0, parts[2].indexOf(FSDataset.METADATA_EXTENSION));
      try{
        return Long.parseLong(s);
      }catch(NumberFormatException e){
       return -1;
      }
    }
    
    public static Block getCompressBlock(File blockFile, long gs){
      String blockName = blockFile.getName();
      String[] vals = blockName.split("_");
      if (vals.length != 4) {
        return null;
      }
      return new Block(Long.valueOf(vals[1]), Long.valueOf(vals[2]), gs);
    }
    
    public static String getBlockPrefixByCdataName(String name){
      String[] strs = name.split("_");
      if(strs == null || strs.length != 4){
        return null;
      }
      return  "blk_" + strs[1];
    }
    
    public static void cleanupCompressData(final Block b, final File pdir){
      String idx = BlockCompressor.getIdxFile(pdir, b.getBlockId()).getAbsolutePath();
      String cdata = BlockCompressor.getCompressDataFile(pdir, b.getBlockId(), b.getNumBytes(), 0).getAbsolutePath();
      new File(idx).delete();
      new File(cdata).delete();
      File[] files = pdir.listFiles(new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            return pathname.getName().matches(pdir.getAbsolutePath() + "/" + b.getBlockName() + "_[0-9]*.meta");
        }
      });
      if(files == null)
        return;
      for(File f : files){
        long gs = CompressUtils.getGenerationStampFromMeta(f);
        if(gs < b.getGenerationStamp()){
          LOG.info("try to delete file " + f.getAbsolutePath());
          f.delete(); 
        }
      }
    }
    
    public static String getCompressBlockFilePath(String blockFileName, int codec_type, Block b){
      return blockFileName + "_" + b.getNumBytes() + "_" + codec_type + FSConstants.CDATA_EXTENSION;
    }
}

