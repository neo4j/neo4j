package org.neo4j.kernel.ha.comm;

import org.jboss.netty.buffer.ChannelBuffer;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.IdAllocation;
import org.neo4j.kernel.ha.LockResult;
import org.neo4j.kernel.ha.LockStatus;
import org.neo4j.kernel.ha.SlaveContext;
import org.neo4j.kernel.impl.nioneo.store.IdRange;

class CommunicationUtils
{
    public static void writeString( String string, ChannelBuffer buffer, boolean isLong )
    {
        char[] data = string.toCharArray();
        if ( isLong )
        {
            buffer.writeShort( data.length );
        }
        else
        {
            buffer.writeByte( data.length );
        }
        for ( char chr : data )
        {
            buffer.writeChar( chr );
        }
    }

    public static String readString( ChannelBuffer buffer, int length )
    {
        char[] data = new char[length];
        for ( int i = 0; i < data.length; i++ )
        {
            data[i] = buffer.readChar();
        }
        return new String( data );
    }

    public static void writeLockResult( LockResult result, ChannelBuffer buffer )
    {
        buffer.writeByte( result.getStatus().ordinal() );
        if ( result.getStatus().hasMessage() )
        {
            writeString( result.getDeadlockMessage(), buffer, true );
        }
    }

    public static LockResult readLockResult( ChannelBuffer buffer )
    {
        LockStatus status = LockStatus.values()[buffer.readByte()];
        return status.hasMessage() ? new LockResult(
                readString( buffer, buffer.readUnsignedShort() ) ) : new LockResult( status );
    }

    public static void writeIdAllocation( IdAllocation alloc, ChannelBuffer buffer )
    {
        IdRange idRange = alloc.getIdRange();
        buffer.writeInt( idRange.getDefragIds().length );
        for ( long id : idRange.getDefragIds() )
        {
            buffer.writeLong( id );
        }
        buffer.writeLong( idRange.getRangeStart() );
        buffer.writeInt( idRange.getRangeLength() );
        buffer.writeLong( alloc.getHighestIdInUse() );
        buffer.writeLong( alloc.getDefragCount() );
    }

    public static IdAllocation readIdAllocation( ChannelBuffer buffer )
    {
        int numberOfDefragIds = buffer.readInt();
        long[] defragIds = new long[numberOfDefragIds];
        for ( int i = 0; i < numberOfDefragIds; i++ )
        {
            defragIds[i] = buffer.readLong();
        }
        long rangeStart = buffer.readLong();
        int rangeLength = buffer.readInt();
        long highId = buffer.readLong();
        long defragCount = buffer.readLong();
        return new IdAllocation( new IdRange( defragIds, rangeStart, rangeLength ), highId,
                defragCount );
    }

    static long[] tryReadIdArray( ChannelBuffer buffer )
    {
        int idCount = buffer.getUnsignedByte( buffer.readerIndex() );
        if ( buffer.readableBytes() < idCount * 8 + 2 ) return null;
        buffer.skipBytes( 2 ); // requestType and idCount
        long[] ids = new long[idCount];
        for ( int i = 0; i < ids.length; i++ )
        {
            ids[i] = buffer.readLong();
        }
        return ids;
    }

    static void writeIdArray( long[] ids, ChannelBuffer buffer )
    {
        buffer.writeByte( ids.length );
        for ( long id : ids )
        {
            buffer.writeLong( id );
        }
    }

    @SuppressWarnings( "boxing" )
    public static void writeSlaveContext( SlaveContext context, ChannelBuffer buffer )
    {
        buffer.writeInt( context.machineId() );
        buffer.writeInt( context.getEventIdentifier() );
        Pair<String, Long>[] txs = context.lastAppliedTransactions();
        buffer.writeByte( txs.length );
        for ( Pair<String, Long> tx : txs )
        {
            writeString( tx.first(), buffer, false );
            buffer.writeLong( tx.other() );
        }
    }

    @SuppressWarnings( "boxing" )
    static SlaveContext tryReadSlaveContext( ChannelBuffer buffer )
    {
        if ( buffer.readableBytes() < 10 ) return null;
        int machineId = buffer.readInt();
        int eventIdentifier = buffer.readInt();
        int txsSize = buffer.readByte();
        @SuppressWarnings( "unchecked" ) Pair<String, Long>[] lastTransactions = new Pair[txsSize];
        for ( int i = 0; i < txsSize; i++ )
        {
            if ( !buffer.readable() ) return null;
            int size = buffer.readUnsignedByte();
            if ( buffer.readableBytes() < ( size * 2 ) + 8 ) return null;
            String name = readString( buffer, size );
            long txId = buffer.readLong();
            lastTransactions[i] = new Pair<String, Long>( name, txId );
        }
        return new SlaveContext( machineId, eventIdentifier, lastTransactions );
    }
}
