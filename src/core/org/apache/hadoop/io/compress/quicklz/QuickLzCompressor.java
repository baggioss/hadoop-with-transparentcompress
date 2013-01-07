package org.apache.hadoop.io.compress.quicklz;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.NativeCodeLoader;

public class QuickLzCompressor implements Compressor {

	private Buffer uncompressedDirectBuf = null;
	private Buffer compressedDirectBuf = null;
	private boolean finished = false;
    private int bytesRead = 0;

	private static boolean nativeQuickLzLoaded = true;
        private static Class clazz = QuickLzCompressor.class;
	private static final Log LOG = LogFactory.getLog(QuickLzCompressor.class
			.getName());

//	static {
//		if (NativeCodeLoader.isNativeCodeLoaded()) {
//			// Initialize the native library
//			try {
//				nativeQuickLzLoaded = true;
//				initIDs();
//				//System.err.println("set nativeQuickLzLoaded to true");
//			} catch (Throwable t) {
//				// Ignore failure to load/initialize native-lzo
//				nativeQuickLzLoaded = false;
//				System.err.println(t.getClass());
//				System.err.println("cannot load quicklz");
//			}
//
//		} else {
//			LOG.error("Cannot load " + QuickLzCompressor.class.getName()
//					+ " without native-hadoop library!");
//			nativeQuickLzLoaded = false;
//		}
//	}


	public QuickLzCompressor(int directBufferSize) {
		uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
		compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize + 400);
		compressedDirectBuf.position(directBufferSize + 400);
	}
	public QuickLzCompressor(ByteBuffer compressedDirectBuf, ByteBuffer uncompressedDirectBuf) {
	  this.uncompressedDirectBuf = uncompressedDirectBuf;
	  this.uncompressedDirectBuf.clear();
	  
	  this.compressedDirectBuf = compressedDirectBuf;
	  this.compressedDirectBuf.clear();
	  this.compressedDirectBuf.position(this.compressedDirectBuf.capacity());
	}

	/**
	 * will be invoked when the uncompressedDirectBuf is full,
	 * and now compressedDirectBuf is empty
	 */
	@Override
	public synchronized int compress(byte[] b, int off, int len) throws IOException {
		if (b == null) {
			throw new NullPointerException();
		}
		if (off < 0 || len < 0 || off > b.length - len) {
			throw new ArrayIndexOutOfBoundsException();
		}
		finished = true;
		int n = compressedDirectBuf.remaining();
		if(n > 0){
		    n = Math.min(n, len);
		    ((ByteBuffer)compressedDirectBuf).get(b, off, n);
		    return n;
		}
		compressedDirectBuf.clear();
		compressedDirectBuf.limit(0);
		if(0 == uncompressedDirectBuf.position())
		    return 0;
		// invoke the native interface
		//n = compress();
		byte data[] = new byte[bytesRead];
		uncompressedDirectBuf.position(0);
		((ByteBuffer)uncompressedDirectBuf).get(data, 0, bytesRead);
		byte []res = QuickLZ.compress(data, 1);
		n = res.length;
        bytesRead = 0;
		compressedDirectBuf.limit(n);
		compressedDirectBuf.position(0);
		((ByteBuffer)compressedDirectBuf).put(res);
		compressedDirectBuf.position(0);
		n = Math.min(n, len);
		((ByteBuffer) compressedDirectBuf).get(b, off, n);
		return n;
	}

	public static boolean isNativeQuickLzLoaded() {
		return nativeQuickLzLoaded;
	}

	
	@Override
	public void end() {
		// noop

	}

	@Override
	public void finish() {
		//noop
	}

	@Override
	public synchronized boolean finished() {
		// noop
		return  finished && 0 == compressedDirectBuf.remaining() && 0 == bytesRead;
	}

	@Override
	public synchronized long getBytesRead() {
		// TODO Auto-generated method stub
		return bytesRead;
		
	}

	@Override
	public long getBytesWritten() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public synchronized boolean needsInput() {
		// TODO Auto-generated method stub
	    return true;
	}

	@Override
	public synchronized void reset() {
		uncompressedDirectBuf.position(0);
		compressedDirectBuf.position(0);
		compressedDirectBuf.limit(0);
		finished = false;
        bytesRead = 0;
	}

	@Override
	public void setDictionary(byte[] b, int off, int len) {
		//noop
	}

	//bytesRead + len will not overflow the buffer
	@Override
	public synchronized void setInput(byte[] b, int off, int len) {
	    if (b == null) {
	        throw new NullPointerException();
	    }
	    if (off < 0 || len < 0 || off > b.length - len) {
	        throw new ArrayIndexOutOfBoundsException();
	    }
        if(len > uncompressedDirectBuf.remaining()) {
			throw new IllegalArgumentException("input will overflow the buffer");
		}
		((ByteBuffer)uncompressedDirectBuf).put(b, off, len);
        bytesRead += len;
		return;
	}

	private native static void initIDs();

	private native int compress();

}

