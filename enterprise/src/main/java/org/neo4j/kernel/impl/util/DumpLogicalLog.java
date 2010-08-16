package org.neo4j.kernel.impl.util;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.nioneo.store.AbstractRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;

public class DumpLogicalLog
{
    public static void main( String args[] ) throws IOException
    {
        String fileName = args[0];
        boolean single = false;
        int startIndex = 0;
        if ( args[0].equals( "-single" ) )
        {
            single = true;
            fileName = args[1];
            startIndex = 1;
        }
        for ( int i = startIndex; i < args.length; i++ )
        {
            fileName = args[i];
            FileChannel fileChannel = new RandomAccessFile( fileName, "r" ).getChannel();
            ByteBuffer buffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE
                    + Xid.MAXBQUALSIZE * 10 );
            buffer.clear();
            if ( !single )
            {
                buffer.limit( 16 );
                if ( fileChannel.read( buffer ) != 16 )
                {
                    System.out.println( "Unable to read timestamp information, "
                        + "no records in logical log." );
                    fileChannel.close();
                    return;
                }
                buffer.flip();
                long logVersion = buffer.getLong();
                long prevLastCommittedTx = buffer.getLong();
                System.out.println( "Logical log version: " + logVersion + " with prev committed tx[" +
                    prevLastCommittedTx + "]" );
            }
            long logEntriesFound = 0;
            XaCommandFactory cf = new CommandFactory();
            while ( readEntry( fileChannel, buffer, cf ) )
            {
                logEntriesFound++;
            }
            System.out.println( "Internal recovery completed, scanned " + logEntriesFound
                + " log entries." );
            fileChannel.close();
        }
    }

    private static boolean readEntry( FileChannel channel, ByteBuffer buf, 
            XaCommandFactory cf ) throws IOException
    {
        LogEntry entry = LogIoUtils.readEntry( buf, channel, cf );
        if ( entry != null )
        {
            System.out.println( entry.toString() );
            return true;
        }
        return false;
    }
    
    private static class CommandFactory extends XaCommandFactory
    {

        @Override
        public XaCommand readCommand( ReadableByteChannel byteChannel,
                ByteBuffer buffer ) throws IOException
        {
            return DumpLogicalLog.readCommand( byteChannel, buffer );
        }
        
    }

    static DynamicRecord readDynamicRecord( ReadableByteChannel byteChannel,
            ByteBuffer buffer ) throws IOException
        {
            // id+type+in_use(byte)+prev_block(int)+nr_of_bytes(int)+next_block(int)
            buffer.clear();
            buffer.limit( 9 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            int id = buffer.getInt();
            int type = buffer.getInt();
            byte inUseFlag = buffer.get();
            boolean inUse = false;
            if ( inUseFlag == Record.IN_USE.byteValue() )
            {
                inUse = true;
                buffer.clear();
                buffer.limit( 12 );
                if ( byteChannel.read( buffer ) != buffer.limit() )
                {
                    return null;
                }
                buffer.flip();
            }
            else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
            {
                throw new IOException( "Illegal in use flag: " + inUseFlag );
            }
            DynamicRecord record = new DynamicRecord( id );
            record.setInUse( inUse, type );
            if ( inUse )
            {
                record.setPrevBlock( buffer.getInt() );
                int nrOfBytes = buffer.getInt();
                record.setNextBlock( buffer.getInt() );
                buffer.clear();
                buffer.limit( nrOfBytes );
                if ( byteChannel.read( buffer ) != buffer.limit() )
                {
                    return null;
                }
                buffer.flip();
                byte data[] = new byte[nrOfBytes];
                buffer.get( data );
                record.setData( data );
            }
            return record;
        }

    // means the first byte of the command record was only written but second 
    // (saying what type) did not get written but the file still got expanded
    private static final byte NONE = (byte) 0;
    
    private static final byte NODE_COMMAND = (byte) 1;
    private static final byte PROP_COMMAND = (byte) 2;
    private static final byte REL_COMMAND = (byte) 3;
    private static final byte REL_TYPE_COMMAND = (byte) 4;
    private static final byte PROP_INDEX_COMMAND = (byte) 5;

    static XaCommand readNodeCommand( ReadableByteChannel byteChannel, ByteBuffer buffer ) 
        throws IOException
    {
        buffer.clear();
        buffer.limit( 5 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        int id = buffer.getInt();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( inUseFlag == Record.IN_USE.byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        NodeRecord record = new NodeRecord( id );
        record.setInUse( inUse );
        if ( inUse )
        {
            buffer.clear();
            buffer.limit( 8 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            record.setNextRel( buffer.getInt() );
            record.setNextProp( buffer.getInt() );
        }
        return new Command( record );
    }
    
    static XaCommand readRelationshipCommand( ReadableByteChannel byteChannel, ByteBuffer buffer ) 
        throws IOException
    {
        buffer.clear();
        buffer.limit( 5 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        int id = buffer.getInt();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE
            .byteValue() )
        {
            inUse = true;
        }
        else if ( (inUseFlag & Record.IN_USE.byteValue()) != Record.NOT_IN_USE
            .byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        RelationshipRecord record;
        if ( inUse )
        {
            buffer.clear();
            buffer.limit( 32 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            record = new RelationshipRecord( id, buffer.getInt(), buffer
                .getInt(), buffer.getInt() );
            record.setInUse( inUse );
            record.setFirstPrevRel( buffer.getInt() );
            record.setFirstNextRel( buffer.getInt() );
            record.setSecondPrevRel( buffer.getInt() );
            record.setSecondNextRel( buffer.getInt() );
            record.setNextProp( buffer.getInt() );
        }
        else
        {
            record = new RelationshipRecord( id, -1, -1, -1 );
            record.setInUse( false );
        }
        return new Command( record );
    }

    static XaCommand readPropertyIndexCommand( ReadableByteChannel byteChannel,
        ByteBuffer buffer ) throws IOException
    {
        // id+in_use(byte)+count(int)+key_blockId(int)+nr_key_records(int)
        buffer.clear();
        buffer.limit( 17 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        int id = buffer.getInt();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE
            .byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        PropertyIndexRecord record = new PropertyIndexRecord( id );
        record.setInUse( inUse );
        record.setPropertyCount( buffer.getInt() );
        record.setKeyBlockId( buffer.getInt() );
        int nrKeyRecords = buffer.getInt();
        for ( int i = 0; i < nrKeyRecords; i++ )
        {
            DynamicRecord dr = readDynamicRecord( byteChannel, buffer );
            if ( dr == null )
            {
                return null;
            }
            record.addKeyRecord( dr );
        }
        return new Command( record );
    }
    
    static XaCommand readPropertyCommand( ReadableByteChannel byteChannel, ByteBuffer buffer ) 
        throws IOException
    {
        // id+in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
        // prev_prop_id(int)+next_prop_id(int)+nr_value_records(int)
        buffer.clear();
        buffer.limit( 9 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        int id = buffer.getInt();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE
            .byteValue() )
        {
            inUse = true;
        }
        boolean nodeProperty = true;
        if ( (inUseFlag & Record.REL_PROPERTY.byteValue() ) == 
            Record.REL_PROPERTY.byteValue() )
        {
            nodeProperty = false;
        }
        int primitiveId = buffer.getInt();
        PropertyRecord record = new PropertyRecord( id );
        if ( primitiveId != -1 && nodeProperty )
        {
            record.setNodeId( primitiveId );
        }
        else if ( primitiveId != -1 )
        {
            record.setRelId( primitiveId );
        }
        if ( inUse )
        {
            buffer.clear();
            buffer.limit( 24 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            PropertyType type = getType( buffer.getInt() );
            if ( type == null )
            {
                return null;
            }
            record.setType( type );
            record.setInUse( inUse );
            record.setKeyIndexId( buffer.getInt() );
            record.setPropBlock( buffer.getLong() );
            record.setPrevProp( buffer.getInt() );
            record.setNextProp( buffer.getInt() );
        }
        buffer.clear();
        buffer.limit( 4 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        int nrValueRecords = buffer.getInt();
        for ( int i = 0; i < nrValueRecords; i++ )
        {
            DynamicRecord dr = readDynamicRecord( byteChannel, buffer );
            if ( dr == null )
            {
                return null;
            }
            record.addValueRecord( dr );
        }
        return new Command( record );
    }

    private static PropertyType getType( int type )
    {
        switch ( type )
        {
            case 1:
                return PropertyType.INT;
            case 2:
                return PropertyType.STRING;
            case 3:
                return PropertyType.BOOL;
            case 4:
                return PropertyType.DOUBLE;
            case 5:
                return PropertyType.FLOAT;
            case 6:
                return PropertyType.LONG;
            case 7:
                return PropertyType.BYTE;
            case 8:
                return PropertyType.CHAR;
            case 9:
                return PropertyType.ARRAY;
            case 10:
                return PropertyType.SHORT;
            case 0:
                return null;
        }
        throw new InvalidRecordException( "Unknown property type:" + type );
    }
    
    static XaCommand readRelationshipTypeCommand( ReadableByteChannel byteChannel, ByteBuffer buffer ) 
        throws IOException
    {
        // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
        buffer.clear();
        buffer.limit( 13 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        int id = buffer.getInt();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( (inUseFlag & Record.IN_USE.byteValue()) == 
            Record.IN_USE.byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        RelationshipTypeRecord record = new RelationshipTypeRecord( id );
        record.setInUse( inUse );
        record.setTypeBlock( buffer.getInt() );
        int nrTypeRecords = buffer.getInt();
        for ( int i = 0; i < nrTypeRecords; i++ )
        {
            DynamicRecord dr = readDynamicRecord( byteChannel, buffer );
            if ( dr == null )
            {
                return null;
            }
            record.addTypeRecord( dr );
        }
        return new Command( record );
    }

    static XaCommand readCommand( ReadableByteChannel byteChannel,
        ByteBuffer buffer ) throws IOException
    {
        buffer.clear();
        buffer.limit( 1 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        byte commandType = buffer.get();
        switch ( commandType )
        {
            case NODE_COMMAND:
                return readNodeCommand( byteChannel, buffer );
            case PROP_COMMAND:
                return readPropertyCommand( byteChannel, buffer );
            case PROP_INDEX_COMMAND:
                return readPropertyIndexCommand( byteChannel, buffer );
            case REL_COMMAND:
                return readRelationshipCommand( byteChannel, buffer );
            case REL_TYPE_COMMAND:
                return readRelationshipTypeCommand( byteChannel, buffer );
            case NONE: return null;
            default:
                throw new IOException( "Unknown command type[" + commandType
                    + "]" );
        }
    }
    
    private static class Command extends XaCommand
    {
        private final AbstractRecord record;
        
        Command( AbstractRecord record )
        {
            this.record = record;
        }
        @Override
        public void execute()
        {
            throw new RuntimeException();
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            throw new RuntimeException();
        }
        
        public String toString()
        {
            return record.toString();
        }
    }
}
