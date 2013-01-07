package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.hdfs.protocol.Block;

public class BlockInfo extends Block {
  long atime;
  
  public BlockInfo(Block blk, long mtime){
    super(blk);
    atime = mtime;
  }  
  public BlockInfo(Block blk){
    super(blk);
    atime = System.currentTimeMillis();
  }

  @Override
  public int compareTo(Block b) {
    if(b instanceof BlockInfo){
      BlockInfo blkinfo = (BlockInfo)b;
      if(atime < blkinfo.atime){
        return -1;
      }else if(atime > blkinfo.atime){
        return 1;
      }else{
        return super.compareTo(blkinfo);
      }
    }else{
      return super.compareTo(b);
    }
  }
}
