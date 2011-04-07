/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.util;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.TreeSet;

import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
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
        for ( String arg : args )
        {
            for ( String fileName : filenamesOf( arg ) )
            {
                System.out.println( "=== " + fileName + " ===" );
                FileChannel fileChannel = new RandomAccessFile( fileName, "r" ).getChannel();
                ByteBuffer buffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE
                        + Xid.MAXBQUALSIZE * 10 );
                long logVersion, prevLastCommittedTx;
                try
                {
                    long[] header = LogIoUtils.readLogHeader( buffer, fileChannel, true );
                    logVersion = header[0];
                    prevLastCommittedTx = header[1];
                }
                catch ( IOException ex )
                {
                    System.out.println( "Unable to read timestamp information, "
                        + "no records in logical log." );
                    System.out.println( ex.getMessage() );
                    fileChannel.close();
                    return;
                }
                System.out.println( "Logical log version: " + logVersion + " with prev committed tx[" +
                    prevLastCommittedTx + "]" );
                long logEntriesFound = 0;
                XaCommandFactory cf = new CommandFactory();
                while ( readEntry( fileChannel, buffer, cf ) )
                {
                    logEntriesFound++;
                }
                fileChannel.close();
            }
        }
    }

    private static String[] filenamesOf( String string )
    {
        File file = new File( string );
        if ( file.isDirectory() )
        {
            File[] files = file.listFiles( new FilenameFilter()
            {
                public boolean accept( File dir, String name )
                {
                    return name.contains( "_logical.log.v" );
                }
            } );
            Collection<String> result = new TreeSet<String>();
            for ( int i = 0; i < files.length; i++ )
            {
                result.add( files[i].getPath() );
            }
            return result.toArray( new String[result.size()] );
        }
        else
        {
            return new String[] { string };
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
        // id+type+in_use(byte)+prev_block(long)+nr_of_bytes(int)+next_block(long)
        buffer.clear();
        buffer.limit( 13 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        long id = buffer.getLong();
        int type = buffer.getInt();
        byte inUseFlag = buffer.get();
        boolean inUse = false;
        if ( inUseFlag == Record.IN_USE.byteValue() )
        {
            inUse = true;
            buffer.clear();
            buffer.limit( 20 );
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
            record.setPrevBlock( buffer.getLong() );
            int nrOfBytes = buffer.getInt();
            record.setNextBlock( buffer.getLong() );
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
        buffer.limit( 9 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        long id = buffer.getLong();
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
            buffer.limit( 16 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            record.setNextRel( buffer.getLong() );
            record.setNextProp( buffer.getLong() );
        }
        return new Command( record );
    }

    static XaCommand readRelationshipCommand( ReadableByteChannel byteChannel, ByteBuffer buffer )
        throws IOException
    {
        buffer.clear();
        buffer.limit( 9 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        long id = buffer.getLong();
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
            buffer.limit( 52 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            record = new RelationshipRecord( id, buffer.getInt(), buffer
                .getInt(), buffer.getInt() );
            record.setInUse( inUse );
            record.setFirstPrevRel( buffer.getLong() );
            record.setFirstNextRel( buffer.getLong() );
            record.setSecondPrevRel( buffer.getLong() );
            record.setSecondNextRel( buffer.getLong() );
            record.setNextProp( buffer.getLong() );
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
        // prev_prop_id(long)+next_prop_id(long)+nr_value_records(int)
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
            buffer.limit( 32 );
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
            record.setPrevProp( buffer.getLong() );
            record.setNextProp( buffer.getLong() );
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
        return PropertyType.getPropertyType( type, true );
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
        private final Object record;

        Command( Object record )
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

        @Override
        public String toString()
        {
            return record.toString();
        }
    }
}
