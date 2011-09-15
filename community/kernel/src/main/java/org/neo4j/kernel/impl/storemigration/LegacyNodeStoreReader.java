package org.neo4j.kernel.impl.storemigration;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import static org.neo4j.kernel.impl.storemigration.LegacyStore.*;

public class LegacyNodeStoreReader
{

    public Iterable<NodeRecord> readNodeStore( String fileName ) throws IOException
    {
        FileChannel fileChannel = new RandomAccessFile( fileName, "r" ).getChannel();
        int recordLength = 9;
        int endHeaderSize = UTF8.encode( FROM_VERSION ).length;
        long recordCount = (fileChannel.size() - endHeaderSize) / recordLength;

        ByteBuffer buffer = ByteBuffer.allocateDirect( recordLength );

        ArrayList<NodeRecord> records = new ArrayList<NodeRecord>();
        for ( long id = 0; id < recordCount; id++ )
        {
            buffer.position( 0 );
            fileChannel.read( buffer );
            buffer.flip();
            long inUseByte = buffer.get();

            boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
            if ( inUse )
            {
                long nextRel = getUnsignedInt( buffer );
                long nextProp = getUnsignedInt( buffer );

                long relModifier = (inUseByte & 0xEL) << 31;
                long propModifier = (inUseByte & 0xF0L) << 28;

                NodeRecord nodeRecord = new NodeRecord( id );
                nodeRecord.setInUse( inUse );
                nodeRecord.setNextRel( longFromIntAndMod( nextRel, relModifier ) );
                nodeRecord.setNextProp( longFromIntAndMod( nextProp, propModifier ) );

                records.add( nodeRecord );
            }
        }
        return records;
    }

}
