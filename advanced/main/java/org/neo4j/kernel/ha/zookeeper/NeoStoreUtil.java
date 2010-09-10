package org.neo4j.kernel.ha.zookeeper;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NeoStoreUtil
{
    private static final int RECORD_SIZE = 9;

    private final long creationTime;
    private final long storeId;
    private final long txId;
    private final long version;
    
    public NeoStoreUtil( String storeDir )
    {
        try
        {
            FileChannel fileChannel = new RandomAccessFile( storeDir + "/neostore", "r" ).getChannel();
            ByteBuffer buf = ByteBuffer.allocate( 4*9 );
            if ( fileChannel.read( buf ) != 4*9 )
            {
                throw new RuntimeException( "Unable to read neo store header information" );
            }
            buf.flip();
            buf.get(); // in use byte
            creationTime = buf.getLong();
            buf.get(); // in use byte
            storeId = buf.getLong();
            buf.get(); 
            version = buf.getLong(); // skip log version
            buf.get(); // in use byte
            txId = buf.getLong();
            fileChannel.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    public long getCreationTime()
    {
        return creationTime;
    }
    
    public long getStoreId()
    {
        return storeId;
    }
    
    public long getLastCommittedTx()
    {
        return txId;
    }
    
    public long getVersion()
    {
        return version;
    }
}
