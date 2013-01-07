package org.apache.hadoop.io.compress.quicklz;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.NativeCodeLoader;


public class QuickLzDecompressor implements Decompressor {
    
	private static class NotImplementedException extends RuntimeException {
		public NotImplementedException() {
			super("this method is not implemented");
		}
	}
	private static boolean nativeQuickLzLoaded = true;
	private static final Log LOG = LogFactory.getLog(QuickLzDecompressor.class
			.getName());
	private ByteBuffer compressedDirectBuf = null;
	private ByteBuffer uncompressedDirectBuf = null;
	private static Class clazz = QuickLzDecompressor.class;
	
	//load the quicklz so
//	static {
//		if (NativeCodeLoader.isNativeCodeLoaded()) {
//			// Initialize the native library
//			try {
//				initIDs();
//				nativeQuickLzLoaded = true;
//			} catch (Throwable t) {
//				// Ignore failure to load/initialize native-lzo
//				nativeQuickLzLoaded = false;
//			}
//
//		} else {
//			LOG.error("Cannot load " + QuickLzDecompressor.class.getName()
//					+ " without native-hadoop library!");
//			nativeQuickLzLoaded = false;
//		}
//	}
	
	
	public QuickLzDecompressor(int directBufferSize) {
		if(directBufferSize <= 0) {
			throw new IllegalArgumentException("directBufferSize should be larger than zero");
		}
		compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize + 400);
		uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
		uncompressedDirectBuf.position(directBufferSize);
	}
	
	public QuickLzDecompressor(ByteBuffer compressedDirectBuf,
      ByteBuffer uncompressedDirectBuf) {
	  if (compressedDirectBuf == null || uncompressedDirectBuf == null) {
	    throw new IllegalArgumentException("buffer should not be null.");
	  }
	  this.compressedDirectBuf = compressedDirectBuf;
	  this.compressedDirectBuf.clear();
	  
	  this.uncompressedDirectBuf = uncompressedDirectBuf;
	  this.uncompressedDirectBuf.clear();
	  this.uncompressedDirectBuf.position(this.uncompressedDirectBuf.capacity());
  }

  @Override
	public synchronized int decompress(byte[] b, int off, int len) throws IOException {
		if(b == null) {
			throw new NullPointerException("b should not be null");
		}
		if((off < 0)||(len < 0) ||((off + len) > b.length)) {
			throw new ArrayIndexOutOfBoundsException();
		}
		//if the remaining data is enough
		int n = uncompressedDirectBuf.remaining();
        if(n > 0)
		{
		    n = Math.min(n, len);
		    ((ByteBuffer)uncompressedDirectBuf).get(b, off, n);
		    return n;
		}
		if(0 == compressedDirectBuf.position()){
		    return 0;
		}
		//n = decompress();
		byte []data = new byte[compressedDirectBuf.position()];
		compressedDirectBuf.position(0);
		compressedDirectBuf.get(data);
		byte []res = QuickLZ.decompress(data);
		compressedDirectBuf.position(0);
		n = res.length;
		uncompressedDirectBuf.limit(n);
		uncompressedDirectBuf.position(0);
		((ByteBuffer)uncompressedDirectBuf).put(res);
		uncompressedDirectBuf.position(0);
		n = Math.min(n, len);
		((ByteBuffer)uncompressedDirectBuf).get(b, off, n);
		return n;
	}	

	@Override
	public synchronized void setInput(byte[] b, int off, int len) {
		if(b == null) {
			throw new NullPointerException();
		}
		if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }
        if(len > compressedDirectBuf.remaining()) {
            throw new IllegalArgumentException("input will overflow the buffer");
        }
		//put the data
		((ByteBuffer)this.compressedDirectBuf).put(b, off, len);
	}

	public int getUnCompressedBytes()
    {
        return uncompressedDirectBuf.remaining();
    }

	public static boolean isNativeQuickLzLoaded() {
		return nativeQuickLzLoaded;
	}
	
	private native static void initIDs();

	private native int decompress();
	
	//here are all not implemented method
	
	@Override
	public void end() {
	}

	@Override
	public synchronized boolean finished() {
		return needsInput();
	}

	@Override
	public boolean needsDictionary() {
		return false;
	}

	@Override
	public synchronized boolean needsInput() {
	    return 0 == uncompressedDirectBuf.remaining() && compressedDirectBuf.remaining() > 0;
	}

	@Override
	public void reset() {
		compressedDirectBuf.position(0);
		uncompressedDirectBuf.position(0);
		uncompressedDirectBuf.limit(0);
	}

	@Override
	public void setDictionary(byte[] b, int off, int len) {
		throw new NotImplementedException();
	}

}

