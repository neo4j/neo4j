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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.logging.Logger;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

/**
 * Command implementations for all the commands that can be performed on a Neo
 * store.
 */
public abstract class Command extends XaCommand
{
    static Logger logger = Logger.getLogger( Command.class.getName() );

    private final long key;

    Command( long key )
    {
        this.key = key;
    }

    @Override
    protected void setRecovered()
    {
        super.setRecovered();
    }

    long getKey()
    {
        return key;
    }

    @Override
    public int hashCode()
    {
        return (int) (( key >>> 32 ) ^ key );
    }

    private static void writePropertyBlock( LogBuffer buffer,
            PropertyBlock block ) throws IOException
    {
        if ( !block.inUse() )
        {
            buffer.put( (byte) 0 );
            // return;
        }
        else
        {
            buffer.put( (byte) 1 );
        }
        byte blockSize = (byte) block.getSize();
        if ( blockSize <= 0 )
        {
            throw new IllegalStateException(
                    blockSize + " for PropertyBlock " + block
                            + " is not a valid value for block size of a PropertyBlock in use" );
        }
        buffer.put( blockSize );
        for ( long value : block.getValueBlocks() )
        {
            buffer.putLong( value );
        }
        if ( block.isLight() )
        {
            /*
             *  This has to be int. If this record is not light
             *  then we have the number of DynamicRecords that follow,
             *  which is an int. We do not currently want/have a flag bit so
             *  we simplify by putting an int here always
             */
            buffer.putInt( 0 );
        }
        else
        {
            buffer.putInt( block.getValueRecords().size() );
            for ( DynamicRecord record : block.getValueRecords() )
            {
                writeDynamicRecord( buffer, record );
            }
        }
    }

    static void writeDynamicRecord( LogBuffer buffer, DynamicRecord record )
        throws IOException
    {
        // id+type+in_use(byte)+nr_of_bytes(int)+next_block(long)
        if ( record.inUse() )
        {
            byte inUse = Record.IN_USE.byteValue();
            buffer.putLong( record.getId() ).putInt( record.getType() ).put(
                    inUse ).putInt(
                record.getLength() ).putLong( record.getNextBlock() );
            if ( !record.isLight() )
            {
                byte[] data = record.getData();
                buffer.put( data );
            }
        }
        else
        {
            byte inUse = Record.NOT_IN_USE.byteValue();
            buffer.putLong( record.getId() ).putInt( record.getType() ).put(
                inUse );
        }
    }

    static PropertyBlock readPropertyBlock( ReadableByteChannel byteChannel,
            ByteBuffer buffer ) throws IOException
    {
        PropertyBlock toReturn = new PropertyBlock();
        // Read in block size.
        buffer.clear();
        buffer.limit( 2 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();

        byte inUse = buffer.get();
        if ( inUse == 0 )
        {
            toReturn.setInUse( false );
            // return toReturn;
        }
        else if ( inUse == 1 )
        {
            toReturn.setInUse( true );
        }
        else
        {
            throw new IllegalStateException(
                    inUse + " is not a valid value for the inUse field" );
        }
        int blockSize = buffer.get(); // the size is stored in bytes
        if ( blockSize <= 0 )
        {
            throw new IllegalStateException(
                    "blockSize field was read in as "
                            + blockSize
                            + " for a property block record that was marked as in use" );
        }
        // Read in blocks
        buffer.clear();
        /*
         * We add 4 to avoid another limit()/read() for the DynamicRecord size
         * field later on
         */
        buffer.limit( blockSize + 4 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        long[] blocks = readLongs( buffer, blockSize / 8 );

        /*
         *  Ok, now we may be ready to return, if there are no DynamicRecords. So
         *  we start building the Object
         */
        toReturn.setValueBlocks( blocks );

        /*
         * Read in existence of DynamicRecords. Remember, this has already been
         * read in the buffer with the blocks, above.
         */
        int noOfDynRecs = buffer.getInt();
        if ( noOfDynRecs == 0 )
        {
            // isLight() so we are done
            return toReturn;
        }
        for ( int i = 0; i < noOfDynRecs; i++ )
        {
            DynamicRecord dr = readDynamicRecord( byteChannel, buffer );
            if ( dr == null )
            {
                return null;
            }
            toReturn.addValueRecord( dr );
        }
        return toReturn;
    }

    static DynamicRecord readDynamicRecord( ReadableByteChannel byteChannel,
        ByteBuffer buffer ) throws IOException
    {
        // id+type+in_use(byte)+nr_of_bytes(int)+next_block(long)
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
            int nrOfBytes = buffer.getInt();
            assert nrOfBytes > 0;
            assert nrOfBytes < ( ( 1 << 24 ) - 1 );
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

    private static long[] readLongs( ByteBuffer buffer, int count )
    {
        long[] result = new long[count];
        for ( int i = 0; i < count; i++ )
        {
            result[i] = buffer.getLong();
        }
        return result;
    }

    // means the first byte of the command record was only written but second
    // (saying what type) did not get written but the file still got expanded
    private static final byte NONE = (byte) 0;

    private static final byte NODE_COMMAND = (byte) 1;
    private static final byte PROP_COMMAND = (byte) 2;
    private static final byte REL_COMMAND = (byte) 3;
    private static final byte REL_TYPE_COMMAND = (byte) 4;
    private static final byte PROP_INDEX_COMMAND = (byte) 5;

    static class NodeCommand extends Command
    {
        private final NodeRecord record;
        private final NodeStore store;

        NodeCommand( NodeStore store, NodeRecord record )
        {
            super( record.getId() );
            this.record = record;
            this.store = store;
        }

        @Override
        boolean isCreated()
        {
            return record.isCreated();
        }

        @Override
        boolean isDeleted()
        {
            return !record.inUse();
        }

        @Override
        public void execute()
        {
            if ( isRecovered() )
            {
                logger.fine( this.toString() );
                store.updateRecord( record, true );
            }
            else
            {
                store.updateRecord( record );
            }
        }

        @Override
        public String toString()
        {
            return "NodeCommand[" + record + "]";
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
            buffer.put( NODE_COMMAND );
            buffer.putLong( record.getId() );
            buffer.put( inUse );
            if ( record.inUse() )
            {
                buffer.putLong( record.getNextRel() ).putLong(
                    record.getNextProp() );
            }
        }

        public static Command readCommand( NeoStore neoStore,
            ReadableByteChannel byteChannel, ByteBuffer buffer )
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
            return new NodeCommand( neoStore == null ? null : neoStore.getNodeStore(), record );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( !(o instanceof NodeCommand) )
            {
                return false;
            }
            return getKey() == ((Command) o).getKey();
        }
    }

    static class RelationshipCommand extends Command
    {
        private final RelationshipRecord record;
        private final RelationshipStore store;

        RelationshipCommand( RelationshipStore store, RelationshipRecord record )
        {
            super( record.getId() );
            this.record = record;
            this.store = store;
        }

        @Override
        boolean isCreated()
        {
            return record.isCreated();
        }

        @Override
        boolean isDeleted()
        {
            return !record.inUse();
        }

        long getFirstNode()
        {
            return record.getFirstNode();
        }

        long getSecondNode()
        {
            return record.getSecondNode();
        }

        boolean isRemove()
        {
            return !record.inUse();
        }

        @Override
        public void execute()
        {
            if ( isRecovered() )
            {
                logger.fine( this.toString() );
                store.updateRecord( record, true );
            }
            else
            {
                store.updateRecord( record );
            }
        }

        @Override
        public String toString()
        {
            return "RelationshipCommand[" + record + "]";
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
            buffer.put( REL_COMMAND );
            buffer.putLong( record.getId() );
            buffer.put( inUse );
            if ( record.inUse() )
            {
                buffer.putLong( record.getFirstNode() ).putLong(
                    record.getSecondNode() ).putInt( record.getType() ).putLong(
                    record.getFirstPrevRel() )
                    .putLong( record.getFirstNextRel() ).putLong(
                        record.getSecondPrevRel() ).putLong(
                        record.getSecondNextRel() ).putLong(
                        record.getNextProp() );
            }
        }

        public static Command readCommand( NeoStore neoStore,
            ReadableByteChannel byteChannel, ByteBuffer buffer )
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
                buffer.limit( 60 );
                if ( byteChannel.read( buffer ) != buffer.limit() )
                {
                    return null;
                }
                buffer.flip();
                record = new RelationshipRecord( id, buffer.getLong(), buffer
                    .getLong(), buffer.getInt() );
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
            return new RelationshipCommand( neoStore == null ? null : neoStore.getRelationshipStore(),
                record );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( !(o instanceof RelationshipCommand) )
            {
                return false;
            }
            return getKey() == ((Command) o).getKey();
        }
    }

    static class PropertyIndexCommand extends Command
    {
        private final PropertyIndexRecord record;
        private final PropertyIndexStore store;

        PropertyIndexCommand( PropertyIndexStore store,
            PropertyIndexRecord record )
        {
            super( record.getId() );
            this.record = record;
            this.store = store;
        }

        @Override
        boolean isCreated()
        {
            return record.isCreated();
        }

        @Override
        boolean isDeleted()
        {
            return !record.inUse();
        }

        @Override
        public void execute()
        {
            if ( isRecovered() )
            {
                logger.fine( this.toString() );
                store.updateRecord( record, true );
            }
            else
            {
                store.updateRecord( record );
            }
        }

        @Override
        public String toString()
        {
            return "PropertyIndexCommand[" + record + "]";
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            // id+in_use(byte)+count(int)+key_blockId(int)+nr_key_records(int)
            byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
            buffer.put( PROP_INDEX_COMMAND );
            buffer.putInt( record.getId() );
            buffer.put( inUse );
            buffer.putInt( record.getPropertyCount() ).putInt(
                record.getKeyBlockId() );
            if ( record.isLight() )
            {
                buffer.putInt( 0 );
            }
            else
            {
                Collection<DynamicRecord> keyRecords = record.getKeyRecords();
                buffer.putInt( keyRecords.size() );
                for ( DynamicRecord keyRecord : keyRecords )
                {
                    writeDynamicRecord( buffer, keyRecord );
                }
            }
        }

        public static Command readCommand( NeoStore neoStore, ReadableByteChannel byteChannel,
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
            return new PropertyIndexCommand( neoStore == null ? null : neoStore.getPropertyStore()
                .getIndexStore(), record );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( !(o instanceof PropertyIndexCommand) )
            {
                return false;
            }
            return getKey() == ((Command) o).getKey();
        }
    }

    static class PropertyCommand extends Command
    {
        private final PropertyRecord record;
        private final PropertyStore store;

        PropertyCommand( PropertyStore store, PropertyRecord record )
        {
            super( record.getId() );
            this.record = record;
            this.store = store;
        }

        @Override
        boolean isCreated()
        {
            return record.isCreated();
        }

        @Override
        boolean isDeleted()
        {
            return !record.inUse();
        }

        @Override
        public void execute()
        {
            if ( isRecovered() )
            {
                logger.fine( this.toString() );
                store.updateRecord( record, true );
            }
            else
            {
                store.updateRecord( record );
            }
        }

        public long getNodeId()
        {
            return record.getNodeId();
        }

        public long getRelId()
        {
            return record.getRelId();
        }

        @Override
        public String toString()
        {
            return "PropertyCommand[" + record + "]";
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
            if ( record.getRelId() != -1 )
            {
                inUse += Record.REL_PROPERTY.byteValue();
            }
            buffer.put( PROP_COMMAND );
            buffer.putLong( record.getId() );
            buffer.put( inUse );
            long nodeId = record.getNodeId();
            long relId = record.getRelId();
            if ( nodeId != -1 )
            {
                buffer.putLong( nodeId );
            }
            else if ( relId != -1 )
            {
                buffer.putLong( relId );
            }
            else
            {
                // means this records value has not changed, only place in
                // prop chain
                buffer.putLong( -1 );
            }
            if ( record.inUse() )
            {
                buffer.putLong( record.getNextProp() ).putLong( record.getPrevProp() );
                int propBlocks = record.getPropertyBlocks().size();
                if ( propBlocks <= 0 )
                {
                    throw new IllegalStateException(
                            "Non positive number of property blocks ("
                                    + propBlocks
                                    + ") for in use property record " + record );
                }
                buffer.putInt( propBlocks );
                for ( PropertyBlock block : record.getPropertyBlocks() )
                {
                    writePropertyBlock( buffer, block );
                }
            }
        }

        public static Command readCommand( NeoStore neoStore,
            ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
        {
            // id+in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
            // prev_prop_id(long)+next_prop_id(long)+nr_value_records(int)
            buffer.clear();
            buffer.limit( 17 );
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
            boolean nodeProperty = true;
            if ( (inUseFlag & Record.REL_PROPERTY.byteValue() ) ==
                Record.REL_PROPERTY.byteValue() )
            {
                nodeProperty = false;
            }
            long primitiveId = buffer.getLong();
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
                /*
                 * 16 next & prev, 4 is the no of property blocks
                 */
                buffer.limit( 16 + 4 );
                if ( byteChannel.read( buffer ) != buffer.limit() )
                {
                    return null;
                }
                buffer.flip();
                // /*byte category = */buffer.get();
                long nextProp = buffer.getLong();
                long prevProp = buffer.getLong();
                record.setNextProp( nextProp );
                record.setPrevProp( prevProp );
                int nrPropBlocks = buffer.getInt();
                if ( nrPropBlocks == 0 )
                {
                    throw new IllegalStateException(
                            "Number of blocks reported for record " + id
                                    + " which is in use was "
                                    + nrPropBlocks );
                }
                while ( nrPropBlocks-- > 0 )
                {
                    PropertyBlock block = readPropertyBlock( byteChannel,
                            buffer );
                    if ( block == null )
                    {
                        return null;
                    }
                    record.addPropertyBlock( block );
                    if ( block.inUse() )
                    {
                        record.setInUse( true );
                    }
                }
                if ( !record.inUse() )
                {
                    throw new IllegalStateException(
                            "Read in record marked as in use but no blocks read in:\n\t"
                                    + record );
                }
            }
            return new PropertyCommand( neoStore == null ? null : neoStore.getPropertyStore(), record );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( !(o instanceof PropertyCommand) )
            {
                return false;
            }
            return getKey() == ((Command) o).getKey();
        }
    }

    static class RelationshipTypeCommand extends Command
    {
        private final RelationshipTypeRecord record;
        private final RelationshipTypeStore store;

        RelationshipTypeCommand( RelationshipTypeStore store,
            RelationshipTypeRecord record )
        {
            super( record.getId() );
            this.record = record;
            this.store = store;
        }

        @Override
        boolean isCreated()
        {
            return record.isCreated();
        }

        @Override
        boolean isDeleted()
        {
            return !record.inUse();
        }

        @Override
        public void execute()
        {
            if ( isRecovered() )
            {
                logger.fine( this.toString() );
                store.updateRecord( record, true );
            }
            else
            {
                store.updateRecord( record );
            }
        }

        @Override
        public String toString()
        {
            return "RelationshipTypeCommand[" + record + "]";
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
            byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
            buffer.put( REL_TYPE_COMMAND );
            buffer.putInt( record.getId() ).put( inUse ).putInt(
                record.getTypeBlock() );

            Collection<DynamicRecord> typeRecords = record.getTypeRecords();
            buffer.putInt( typeRecords.size() );
            for ( DynamicRecord typeRecord : typeRecords )
            {
                writeDynamicRecord( buffer, typeRecord );
            }
        }

        public static Command readCommand( NeoStore neoStore,
            ReadableByteChannel byteChannel, ByteBuffer buffer )
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
            return new RelationshipTypeCommand(
                    neoStore == null ? null : neoStore.getRelationshipTypeStore(), record );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( !(o instanceof RelationshipTypeCommand) )
            {
                return false;
            }
            return getKey() == ((Command) o).getKey();
        }
    }

    public static Command readCommand( NeoStore neoStore, ReadableByteChannel byteChannel,
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
                return NodeCommand.readCommand( neoStore, byteChannel, buffer );
            case PROP_COMMAND:
                return PropertyCommand.readCommand( neoStore, byteChannel,
                    buffer );
            case PROP_INDEX_COMMAND:
                return PropertyIndexCommand.readCommand( neoStore, byteChannel,
                    buffer );
            case REL_COMMAND:
                return RelationshipCommand.readCommand( neoStore, byteChannel,
                    buffer );
            case REL_TYPE_COMMAND:
                return RelationshipTypeCommand.readCommand( neoStore,
                    byteChannel, buffer );
            case NONE: return null;
            default:
                throw new IOException( "Unknown command type[" + commandType
                    + "]" );
        }
    }

    abstract boolean isCreated();

    abstract boolean isDeleted();
}