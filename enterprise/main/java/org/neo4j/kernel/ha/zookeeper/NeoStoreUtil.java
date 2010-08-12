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
    
    public NeoStoreUtil( String storeDir )
    {
        try
        {
            FileChannel fileChannel = new RandomAccessFile( storeDir + "/neostore", "r" ).getChannel();
            ByteBuffer buf = ByteBuffer.allocate( 32 );
            if ( fileChannel.read( buf ) != 32 )
            {
                throw new RuntimeException( "Unable to read neo store header information" );
            }
            buf.flip();
            creationTime = buf.getLong();
            storeId = buf.getLong();
            buf.getLong(); // skip log version
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
}
