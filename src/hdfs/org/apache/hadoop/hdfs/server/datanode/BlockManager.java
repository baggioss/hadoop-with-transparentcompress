/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;

public class BlockManager {
  public static final Log LOG = LogFactory.getLog(BlockManager.class);
  TreeSet<BlockInfo> needCompressQueue;
  HashMap<Block, BlockInfo> blockMap;
  ComressLogHandler handler = null;
  AtimeHandler atimehandler = null;
  Configuration conf;
  long coldDataThredshould = Long.MAX_VALUE, minAccessTimePrecision;
  
  public BlockManager(Configuration conf) {
    this.conf = conf;
    String path = conf.get("hadoop.tmp.dir");
    if(path != null && !path.isEmpty()){
      File f = new File(path);
      if(!f.exists()){
        boolean r = f.mkdirs();
        LOG.info("path " + f.getAbsolutePath() + " is not exist. create it. ret " + r);
      }
    }    
    needCompressQueue = new TreeSet<BlockInfo>();
    blockMap = new HashMap<Block, BlockInfo>();
    coldDataThredshould = conf.getInt("cold.data.threshold.day", 7) * 3600 * 24 * 1000;
    minAccessTimePrecision = conf.getLong("min.dfs.access.time.precision", 1000 * 60);
  }
  
  synchronized void loadOpLog(){
    if(handler == null)
      handler = new ComressLogHandler();
    if(atimehandler == null)  
      atimehandler = new AtimeHandler();
  }
  synchronized void updateBlockAtime(Block blk){
    updateBlockAtime(blk, now());
  }
  
  synchronized void updateBlockAtime(Block blk, long atime){
    BlockInfo blkInfo = blockMap.get(blk);
    if(blkInfo == null){
      LOG.info("can not find block " + blk.toString() + " in block map.");
      blkInfo = new BlockInfo(blk);
      blockMap.put(blk, blkInfo);
      atimehandler.saveAtime(blk, atime);
      return;
    }
    long lastAtime = blkInfo.atime;
    if(atime - lastAtime < minAccessTimePrecision)
      return;
    needCompressQueue.remove(blkInfo);
    blkInfo.atime = atime;
    needCompressQueue.add(blkInfo);
    atimehandler.saveAtime(blk, atime);
  }
  
  synchronized void addBlockAtime(Block blk, long atime){
    BlockInfo blkInfo = blockMap.get(blk);
    if(blkInfo == null){
      LOG.debug("can not find block " + blk.toString() + " in block map.");
      return;
    }
    blkInfo.atime = atime;
    if(now() - atime > coldDataThredshould)
      needCompressQueue.add(blkInfo);
  }
  
  synchronized void remove(Block blk){
    BlockInfo info = blockMap.remove(blk);
    if(info != null){
      needCompressQueue.remove(info);
      handler.removeFromBlackList(blk);
    }
  }
  
  synchronized void setCompressed(Block blk){
    BlockInfo info = blockMap.get(blk);
    if(info != null){
      needCompressQueue.remove(info);
    }
  }
  
  synchronized void setColdDataThredshould(int day){
    coldDataThredshould = day * 3600 * 24 * 1000;
  }
  
  synchronized void unprotectedAdd(Block blk){
    if(handler.isInBlackList(blk)){
      LOG.debug("block " + blk.toString() + " is in black list.");
      return;
    }
      
    BlockInfo info = new BlockInfo(blk);
    blockMap.put(blk, info);
    needCompressQueue.add(info);
  }
  
  synchronized void add(Block blk, long atime){
    unprotectedAdd(blk);
    if(now() - atime > coldDataThredshould){
      if(LOG.isDebugEnabled()){
        LOG.debug("block " + blk.toString() + " atime is " + atime + " has not become to cold data.");
      }
      BlockInfo info = new BlockInfo(blk);
      info.atime = atime;
      needCompressQueue.add(info);
      }
  }
  
  synchronized void adjustByBlockReport(Block blk, long atime){
    if(handler.isInBlackList(blk)){
      if(LOG.isDebugEnabled()){
        LOG.debug("block " + blk.toString() + " is in black list.");
      }
      return;
    }
      
    if (blk.getGenerationStamp() == Block.GRANDFATHER_GENERATION_STAMP)
      return;
    BlockInfo info = blockMap.get(blk);
    if(info == null){
      info = new BlockInfo(blk);
      blockMap.put(blk, info);
      if(now() - atime > coldDataThredshould)
        needCompressQueue.add(info);
    }
  }
  
  /**
   * @return return the first block in needCompressQueue. "return null" means there's not block can be compressed.
   *         dispatcher should wait for a moment. 
   * */
  synchronized Block poll(){
    BlockInfo blk = needCompressQueue.pollFirst();
    if(blk == null)
      return null;
    if(now() - blk.atime < coldDataThredshould){
      if(LOG.isDebugEnabled()){
        LOG.debug("block " + blk.toString() + " atime is " + blk.atime + " has not become to cold data.");
      }
      needCompressQueue.add(blk);
      return null;
    }
    
    blockMap.remove(blk);
    return blk;
  }
  
  synchronized void blackList(Block blk){
    BlockInfo info = blockMap.remove(blk);
    if(info != null)
      needCompressQueue.remove(info);
    try{
      if(!handler.isInBlackList(blk)){
        handler.putInBlackList(blk);
        LOG.info("In getTask() put block into black list : " + blk.toString());
      }
    }catch(IOException e){
      LOG.warn("", e);
    }
  }

  synchronized void blackListForAppend(Block blk){
    BlockInfo info = blockMap.remove(blk);
    if(info != null)
      needCompressQueue.remove(info);
    try{
      handler.putInBlackList(blk);
    }catch(IOException e){
      LOG.warn("", e);
    }
  }
  
  void stop(){
    if(handler != null){
      handler.shutdown();
    }
    if(atimehandler != null){
      atimehandler.shutdown();
    }
  }
  
  static long now(){
    return System.currentTimeMillis();
  }
  
  class ComressLogHandler{
    HashSet<Block> blackList = new HashSet<Block>(); 
    BufferedWriter bw = null;
    String logPath;
    File logFile; 
    long maxCompressLogSize;
    
    public ComressLogHandler() {
      try{
        logPath = conf.get("hadoop.datanode.compress.log", conf.get("hadoop.tmp.dir") + Path.SEPARATOR + "compress.log");
        maxCompressLogSize = conf.getLong("dfs.max.blacklist.log.size", 64 * 1024 * 1024);
        logFile = new File(logPath);
        File pdir = logFile.getParentFile();
        if(!pdir.exists()){
          boolean ret = pdir.mkdirs();
          LOG.info("path " + pdir.getAbsolutePath() + " is not exist. create it. ret " + ret);
        }
        if(!logFile.exists()){
          String tmpPath = getTmpLogPath();
          loadBlackList(getTmpLogPath());
          File f = new File(tmpPath);
          f.renameTo(logFile);
        }else{
          loadBlackList(logPath);
        }
        bw = new BufferedWriter(new FileWriter(logFile, true));
      } catch(IOException e){
        LOG.warn("", e);
      }
    }

    synchronized void loadBlackList(String path){
      File f = new File(path);
      if(!f.exists())
         return;
      BufferedReader fr = null;
      String line = null;
      try{
        fr = new BufferedReader(new FileReader(f));
        while((line = fr.readLine()) != null){
          try{
            String[] strs = line.split(" ");
            long blkId = Long.parseLong(strs[0]);
            long gs = Long.parseLong(strs[1]);
            blackList.add(new Block(blkId, 0, gs));
          }catch(Throwable ignored){}
        }
      }catch(IOException e){
         LOG.warn("load black list error.", e);
      }finally{
       if(fr != null){
         try{
           fr.close();
         }catch(IOException ignore){}
       }
      }
    }
    
    synchronized void putInBlackList(Block blk) throws IOException{
      if(logFile.length() > maxCompressLogSize){
        switchCompressLog();
      }
      blackList.add(blk);
      String blkId = String.valueOf(blk.getBlockId());
      String blkgs = String.valueOf(blk.getGenerationStamp());
      if(bw != null){
        bw.write(blkId + " " + blkgs + "\n");
        bw.flush();
      }
    }
    
    synchronized void switchCompressLog() throws IOException{
      String tmpPath = getTmpLogPath();
      File tmpFile = new File(tmpPath);
      if(tmpFile.exists()){
        LOG.warn("tmp file has been existed, remove it.");
        tmpFile.delete();
      }
      BufferedWriter tmpBw = new BufferedWriter(new FileWriter(tmpFile));
      for(Block blk : blackList){
        String blkId = String.valueOf(blk.getBlockId());
        String blkgs = String.valueOf(blk.getGenerationStamp());
        if(tmpBw != null){
          tmpBw.write(blkId + " " + blkgs + "\n");
        }
      }
      try{
        if(tmpBw != null)
          tmpBw.close();
        if(bw != null)
          bw.close();
      }catch(IOException e){}
      
      boolean ret = tmpFile.renameTo(logFile);
      if(!ret)
        throw new IOException("fail to rename from tmp to log.");
      bw = new BufferedWriter(new FileWriter(logFile));
    }
    
    String getTmpLogPath(){
      return logPath + ".tmp";
    }
    
    synchronized void removeFromBlackList(Block blk){
      if(blackList != null)
        blackList.remove(blk);
    }
    
    synchronized boolean isInBlackList(Block blk){
      if(blackList != null)
        return blackList.contains(blk);
      return false;
    }
    
    synchronized void shutdown(){
      if(bw != null){
        try{
          bw.close();
        }catch(IOException e){
          LOG.info("shut dowm compress log handler", e);
        }
      }
    }
  }
  
  class AtimeHandler{
    BufferedWriter bw = null;
    String logPath;
    File logFile; 
    long maxAtimeLog;
    
    public AtimeHandler() {
        try{
          logPath = conf.get("hadoop.datanode.atime.log", conf.get("hadoop.tmp.dir") + Path.SEPARATOR + "atime.log");
          maxAtimeLog = conf.getLong("dfs.max.atime.log.size", 64 * 1024 * 1024);
          logFile = new File(logPath);
          File pdir = logFile.getParentFile();
          if(!pdir.exists()){
            boolean ret = pdir.mkdirs();
            LOG.info("path " + pdir.getAbsolutePath() + " is not exist. create it. ret " + ret);
          }
          if(!logFile.exists()){
            String tmpPath = getTmpLogPath();
            try{
              loadAtime(getTmpLogPath());
            }catch(IOException e){
              LOG.warn("fail to load black list log.", e);
            }
            File f = new File(tmpPath);
            f.delete();
          }else{
            loadAtime(logPath);
          }
          bw = new BufferedWriter(new FileWriter(logFile, true));
        } catch(IOException e){
          LOG.warn("", e);
        }
    }
    
    String getTmpLogPath(){
      return logPath + ".tmp";
    }
    
    synchronized void saveAtime(Block blk, long atime){
      try{
        if(logFile.length() > maxAtimeLog){
          switchLog(); 
        }
        String blkId = String.valueOf(blk.getBlockId());
        String blkgs = String.valueOf(blk.getGenerationStamp());
        if(bw != null){
          StringBuffer sb = new StringBuffer(blkId);
          sb.append(" ");
          sb.append(blkgs);
          sb.append(" ");
          sb.append(atime);
          sb.append("\n");
          bw.write(sb.toString());
          bw.flush();
        }
      }catch(IOException e){
        LOG.warn("save atime error", e);
      }
    }
     
    synchronized void switchLog() throws IOException{
        String tmpPath = getTmpLogPath();
        File tmpFile = new File(tmpPath);
        if(tmpFile.exists()){
          LOG.warn("tmp file has been existed, remove it.");
          tmpFile.delete();
        }
        BufferedWriter tmpBw = new BufferedWriter(new FileWriter(tmpFile));
        for(Map.Entry<Block, BlockInfo> entry : blockMap.entrySet()){
          Block blk = entry.getKey();
          String blkId = String.valueOf(blk.getBlockId());
          String blkgs = String.valueOf(blk.getGenerationStamp());
          BlockInfo info = entry.getValue();
          if(tmpBw != null && info != null && now() - info.atime < coldDataThredshould){
            StringBuffer sb = new StringBuffer(blkId);
            sb.append(" ");
            sb.append(blkgs);
            sb.append(" ");
            sb.append(info.atime);
            sb.append("\n");
            tmpBw.write(sb.toString());
          }
        }
        try{
          if(tmpBw != null)
            tmpBw.close();
          if(bw != null)
            bw.close();
        }catch(IOException e){}
        
        boolean ret = tmpFile.renameTo(logFile);
        if(!ret)
          throw new IOException("fail to rename from tmp to log.");
        bw = new BufferedWriter(new FileWriter(logFile));
      } 
     
    synchronized void loadAtime(String path) throws IOException{
      File f = new File(path);
      if(!f.exists())
         return;
      BufferedReader fr = new BufferedReader(new FileReader(f));
      String line = null;
      try{
        while((line = fr.readLine()) != null){
          try{
            String[] strs = line.split(" ");
            long blkId = Long.parseLong(strs[0]);
            long gs = Long.parseLong(strs[1]);
            long atime = Long.parseLong(strs[2]);
            addBlockAtime(new Block(blkId, 0, gs), atime);
          }catch(Throwable ignored){}
        }
      }finally{
       if(fr != null)
         fr.close();
      }
    }
    synchronized void shutdown(){
        if(bw != null){
          try{
            bw.close();
          }catch(IOException e){
            LOG.info("shut dowm compress log handler", e);
          }
        }
      }
  }
  
  synchronized void printQueueReport(StringBuilder sb, int limit){
    sb.append("### need compress queue : \n");
    int i = 1;
    for(BlockInfo info : needCompressQueue){
      if(i > limit)
        break;
      sb.append(String.format("name : %-10s : atime , %-15s\n", info.getBlockName(), new Date(info.atime).toString()));
      i++;
    }
    sb.append("\n### black list : \n");
    i = 1;
    for(Block b : handler.blackList){
      if(i > limit)
        break;
      sb.append(String.format("block : %s \n", b.toString()));
      i++;
    }
  }
}
