package org.neo4j.impl.transaction.xaframework;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import javax.transaction.xa.Xid;

public class LogBuffer
{
	private static final int MAPPED_SIZE = 1024*1024*2;
	
	private final FileChannel fileChannel;
	
	private MappedByteBuffer mappedBuffer = null;
	private long mappedStartPosition;
	private final ByteBuffer fallbackBuffer;
	
	LogBuffer( FileChannel fileChannel )
	{
		this.fileChannel = fileChannel;
		getNewMappedBuffer();
		fallbackBuffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE + 
			Xid.MAXBQUALSIZE * 10 );
	}
	
	private void getNewMappedBuffer()
	{
		try
		{
			if ( mappedBuffer != null )
			{
				mappedBuffer.force();
				fileChannel.position( mappedStartPosition + 
					mappedBuffer.position() );
			}
			mappedBuffer = fileChannel.map( MapMode.READ_WRITE, 
				fileChannel.position(), MAPPED_SIZE );
			mappedStartPosition = fileChannel.position();
		}
		catch ( Throwable t )
		{
			mappedBuffer = null;
			mappedStartPosition = -1;
			t.printStackTrace();
		}
	}
	
	public LogBuffer put( byte b ) throws IOException
	{
		if ( mappedBuffer == null || ( MAPPED_SIZE - 
			mappedBuffer.position() ) < 1 )
		{
			getNewMappedBuffer();
			if ( mappedBuffer == null )
			{
				fallbackBuffer.clear();
				fallbackBuffer.put( b );
				fallbackBuffer.flip();
				fileChannel.write( fallbackBuffer );
				return this;
			}
		}
		mappedBuffer.put( b );
		return this;
	}
	
	public LogBuffer putInt( int i ) throws IOException
	{
		if ( mappedBuffer == null || ( MAPPED_SIZE - 
			mappedBuffer.position() ) < 4 )
		{
			getNewMappedBuffer();
			if ( mappedBuffer == null )
			{
				fallbackBuffer.clear();
				fallbackBuffer.putInt( i );
				fallbackBuffer.flip();
				fileChannel.write( fallbackBuffer );
				return this;
			}
		}
		mappedBuffer.putInt( i );
		return this;
	}
	
	public LogBuffer putLong( long l ) throws IOException
	{
		if ( mappedBuffer == null || ( MAPPED_SIZE - 
			mappedBuffer.position() ) < 8 )
		{
			getNewMappedBuffer();
			if ( mappedBuffer == null )
			{
				fallbackBuffer.clear();
				fallbackBuffer.putLong( l );
				fallbackBuffer.flip();
				fileChannel.write( fallbackBuffer );
				return this;
			}
		}
		mappedBuffer.putLong( l );
		return this;
	}
	
	public LogBuffer put( byte[] bytes ) throws IOException
	{
		if ( mappedBuffer == null || ( MAPPED_SIZE - 
			mappedBuffer.position() ) < bytes.length )
		{
			getNewMappedBuffer();
			if ( mappedBuffer == null )
			{
				fallbackBuffer.clear();
				fallbackBuffer.put( bytes );
				fallbackBuffer.flip();
				fileChannel.write( fallbackBuffer );
				return this;
			}
		}
		mappedBuffer.put( bytes );
		return this;
	}
	
	public LogBuffer put( char[] chars ) throws IOException
	{
		if ( mappedBuffer == null || ( MAPPED_SIZE - 
			mappedBuffer.position() ) < ( chars.length * 2 ) )
		{
			getNewMappedBuffer();
			if ( mappedBuffer == null )
			{
				fallbackBuffer.clear();
				fallbackBuffer.asCharBuffer().put( chars );
				fallbackBuffer.flip();
				fileChannel.write( fallbackBuffer );
				return this;
			}
		}
		int oldPos = mappedBuffer.position();
		mappedBuffer.asCharBuffer().put( chars );
		mappedBuffer.position( oldPos + chars.length * 2 );
		return this;
	}
	
	void releaseMemoryMapped() throws IOException
	{
		if ( mappedBuffer != null )
		{
			mappedBuffer.force();
			mappedBuffer = null;
		}
	}
	
	public void force() throws IOException
	{
		if ( mappedBuffer != null )
		{
			mappedBuffer.force();
		}
		fileChannel.force( false );
	}
	
	public long getFileChannelPosition() throws IOException
	{
		if ( mappedBuffer != null )
		{
			return mappedStartPosition + mappedBuffer.position();
		}
		return fileChannel.position();
	}
	
	public FileChannel getFileChannel()
	{
		return fileChannel;
	}
}
