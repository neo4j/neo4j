/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
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
    
    public abstract void accept( CommandRecordVisitor visitor );

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
        byte blockSize = (byte) block.getSize();
        assert blockSize > 0 : blockSize + " is not a valid block size value";
        buffer.put( blockSize ); // 1
        long[] propBlockValues = block.getValueBlocks();
        for ( int k = 0; k < propBlockValues.length; k++ )
        {
            buffer.putLong( propBlockValues[k] );
        }
        /*
         * For each block we need to keep its dynamic record chain if
         * it is just created. Deleted dynamic records are in the property
         * record and dynamic records are never modified. Also, they are
         * assigned as a whole, so just checking the first should be enough.
         */
        if ( block.isLight() || !block.getValueRecords().get( 0 ).isCreated() )
        {
            /*
             *  This has to be int. If this record is not light
             *  then we have the number of DynamicRecords that follow,
             *  which is an int. We do not currently want/have a flag bit so
             *  we simplify by putting an int here always
             */
            buffer.putInt( 0 ); // 4 or
        }
        else
        {
            buffer.putInt( block.getValueRecords().size() ); // 4
            for ( int i = 0; i < block.getValueRecords().size(); i++ )
            {
                DynamicRecord dynRec = block.getValueRecords().get( i );
                writeDynamicRecord( buffer, dynRec );
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
                    inUse ).putInt( record.getLength() ).putLong(
                    record.getNextBlock() );
            byte[] data = record.getData();
            assert data != null;
            buffer.put( data );
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
        buffer.clear();
        buffer.limit( 1 );
        if ( byteChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        byte blockSize = buffer.get(); // the size is stored in bytes // 1
        assert blockSize > 0 && blockSize % 8 == 0 : blockSize
                                                     + " is not a valid block size value";
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
        assert blocks.length == blockSize / 8 : blocks.length
                                                + " longs were read in while i asked for what corresponds to "
                                                + blockSize;
        assert PropertyType.getPropertyType( blocks[0], false ).calculateNumberOfBlocksUsed(
                blocks[0] ) == blocks.length : blocks.length
                                               + " is not a valid number of blocks for type "
                                               + PropertyType.getPropertyType(
                                                       blocks[0], false );
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
        assert noOfDynRecs >= 0 : noOfDynRecs
                                  + " is not a valid value for the number of dynamic records in a property block";
        if ( noOfDynRecs != 0 )
        {
            for ( int i = 0; i < noOfDynRecs; i++ )
            {
                DynamicRecord dr = readDynamicRecord( byteChannel, buffer );
                if ( dr == null )
                {
                    return null;
                }
                dr.setCreated(); // writePropertyBlock always writes only newly
                                 // created chains
                toReturn.addValueRecord( dr );
            }
            assert toReturn.getValueRecords().size() == noOfDynRecs : "read in "
                                                                      + toReturn.getValueRecords().size()
                                                                      + " instead of the proper "
                                                                      + noOfDynRecs;
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
        assert id >= 0 && id <= ( 1l << 36 ) - 1 : id
                                                  + " is not a valid dynamic record id";
        int type = buffer.getInt();
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

        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( inUse, type );
        if ( inUse )
        {
            buffer.clear();
            buffer.limit( 12 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            int nrOfBytes = buffer.getInt();
            assert nrOfBytes >= 0 && nrOfBytes < ( ( 1 << 24 ) - 1 ) : nrOfBytes
                                                                      + " is not valid for a number of bytes field of a dynamic record";
            long nextBlock = buffer.getLong();
            assert ( nextBlock >= 0 && nextBlock <= ( 1l << 36 - 1 ) )
                   || ( nextBlock == Record.NO_NEXT_BLOCK.intValue() ) : nextBlock
                                                                    + " is not valid for a next record field of a dynamic record";
            record.setNextBlock( nextBlock );
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
    private static final byte NEOSTORE_COMMAND = (byte) 6;

    abstract void removeFromCache( TransactionState state );

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
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitNode( record );
        }

        @Override
        void removeFromCache( TransactionState state )
        {
            state.removeNodeFromCache( getKey() );
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
            return record.toString();
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
            NodeRecord record;
            if ( inUse )
            {
                buffer.clear();
                buffer.limit( 16 );
                if ( byteChannel.read( buffer ) != buffer.limit() )
                {
                    return null;
                }
                buffer.flip();
                record = new NodeRecord( id, buffer.getLong(), buffer.getLong() );
            }
            else record = new NodeRecord( id, Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_NEXT_PROPERTY.intValue() );
            record.setInUse( inUse );
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
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitRelationship( record );
        }

        @Override
        void removeFromCache( TransactionState state )
        {
            state.removeRelationshipFromCache( getKey() );
            if ( this.getFirstNode() != -1 || this.getSecondNode() != -1 )
            {
                state.removeNodeFromCache( this.getFirstNode() );
                state.removeNodeFromCache( this.getSecondNode() );
            }
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
            return record.toString();
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
    
    static class NeoStoreCommand extends Command
    {
        private final NeoStoreRecord record;
        private final NeoStore neoStore;

        NeoStoreCommand( NeoStore neoStore, NeoStoreRecord record )
        {
            super( -1 );
            this.neoStore = neoStore;
            this.record = record;
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
            neoStore.setGraphNextProp( record.getNextProp() );
        }
        
        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitNeoStore( record );
        }

        @Override
        void removeFromCache( TransactionState state )
        {
            // no-op
        }

        @Override
        public String toString()
        {
            return record.toString();
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            buffer.put( NEOSTORE_COMMAND ).putLong( record.getNextProp() );
        }

        public static Command readCommand( NeoStore neoStore,
                ReadableByteChannel byteChannel, ByteBuffer buffer )
                throws IOException
        {
            buffer.clear();
            buffer.limit( 8 );
            if ( byteChannel.read( buffer ) != buffer.limit() ) return null;
            buffer.flip();
            long nextProp = buffer.getLong();
            NeoStoreRecord record = new NeoStoreRecord();
            record.setNextProp( nextProp );
            return new NeoStoreCommand( neoStore, record );
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
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitPropertyIndex( record );
        }

        @Override
        void removeFromCache( TransactionState state )
        {
            // no-op
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
            return record.toString();
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
            buffer.putInt( record.getPropertyCount() ).putInt( record.getNameId() );
            if ( record.isLight() )
            {
                buffer.putInt( 0 );
            }
            else
            {
                Collection<DynamicRecord> keyRecords = record.getNameRecords();
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
            record.setNameId( buffer.getInt() );
            int nrKeyRecords = buffer.getInt();
            for ( int i = 0; i < nrKeyRecords; i++ )
            {
                DynamicRecord dr = readDynamicRecord( byteChannel, buffer );
                if ( dr == null )
                {
                    return null;
                }
                record.addNameRecord( dr );
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
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitProperty( record );
        }

        @Override
        void removeFromCache( TransactionState state )
        {
            long nodeId = this.getNodeId();
            long relId = this.getRelId();
            if ( nodeId != -1 )
            {
                state.removeNodeFromCache( nodeId );
            }
            else if ( relId != -1 )
            {
                state.removeRelationshipFromCache( relId );
            }
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
            return record.toString();
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
            buffer.putLong( record.getId() ); // 8
            buffer.put( inUse ); // 1
            buffer.putLong( record.getNextProp() ).putLong(
                    record.getPrevProp() ); // 8 + 8
            long nodeId = record.getNodeId();
            long relId = record.getRelId();
            if ( nodeId != -1 )
            {
                buffer.putLong( nodeId ); // 8 or
            }
            else if ( relId != -1 )
            {
                buffer.putLong( relId ); // 8 or
            }
            else
            {
                // means this records value has not changed, only place in
                // prop chain
                buffer.putLong( -1 ); // 8
            }
            buffer.put( (byte) record.getPropertyBlocks().size() ); // 1
            for ( int i = 0; i < record.getPropertyBlocks().size(); i++ )
            {
                PropertyBlock block = record.getPropertyBlocks().get( i );
                assert block.getSize() > 0 : record + " seems kinda broken";
                writePropertyBlock( buffer, block );
            }
            buffer.putInt( record.getDeletedRecords().size() ); // 4
            for ( int i = 0; i < record.getDeletedRecords().size(); i++ )
            {
                DynamicRecord dynRec = record.getDeletedRecords().get( i );
                writeDynamicRecord( buffer, dynRec );
            }
        }

        public static Command readCommand( NeoStore neoStore,
            ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
        {
            // id+in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
            // prev_prop_id(long)+next_prop_id(long)
            buffer.clear();
            buffer.limit( 8 + 1 + 8 + 8 + 8 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();

            long id = buffer.getLong(); // 8
            PropertyRecord record = new PropertyRecord( id );
            byte inUseFlag = buffer.get(); // 1
            long nextProp = buffer.getLong(); // 8
            long prevProp = buffer.getLong(); // 8
            record.setNextProp( nextProp );
            record.setPrevProp( prevProp );
            boolean inUse = false;
            if ( ( inUseFlag & Record.IN_USE.byteValue() ) == Record.IN_USE.byteValue() )
            {
                inUse = true;
            }
            boolean nodeProperty = true;
            if ( ( inUseFlag & Record.REL_PROPERTY.byteValue() ) == Record.REL_PROPERTY.byteValue() )
            {
                nodeProperty = false;
            }
            long primitiveId = buffer.getLong(); // 8
            if ( primitiveId != -1 && nodeProperty )
            {
                record.setNodeId( primitiveId );
            }
            else if ( primitiveId != -1 )
            {
                record.setRelId( primitiveId );
            }
            buffer.clear();
            buffer.limit( 1 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            int nrPropBlocks = buffer.get(); // 1
            assert nrPropBlocks >= 0;
            if ( nrPropBlocks > 0 )
            {
                record.setInUse( true );
            }
            while ( nrPropBlocks-- > 0 )
            {
                PropertyBlock block = readPropertyBlock( byteChannel, buffer );
                if ( block == null )
                {
                    return null;
                }
                record.addPropertyBlock( block );
            }
            // Time to read in the deleted dynamic records
            buffer.clear();
            buffer.limit( 4 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            int deletedRecords = buffer.getInt(); // 4
            assert deletedRecords >= 0;
            while ( deletedRecords-- > 0 )
            {
                DynamicRecord read = readDynamicRecord( byteChannel, buffer );
                if ( read == null )
                {
                    return null;
                }
                assert !read.inUse() : read + " is kinda weird";
                record.addDeletedRecord( read );
            }

            if ( ( inUse && !record.inUse() ) || ( !inUse && record.inUse() ) )
            {
                throw new IllegalStateException( "Weird, inUse was read in as "
                                                 + inUse
                                                 + " but the record is "
                                                 + record );
            }
            return new PropertyCommand( neoStore == null ? null
                    : neoStore.getPropertyStore(), record );
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
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitRelationshipType( record );
        }

        @Override
        void removeFromCache( TransactionState state )
        {
            // no-op
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
            return record.toString();
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
            byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
            buffer.put( REL_TYPE_COMMAND );
            buffer.putInt( record.getId() ).put( inUse ).putInt( record.getNameId() );

            Collection<DynamicRecord> typeRecords = record.getNameRecords();
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
            record.setNameId( buffer.getInt() );
            int nrTypeRecords = buffer.getInt();
            for ( int i = 0; i < nrTypeRecords; i++ )
            {
                DynamicRecord dr = readDynamicRecord( byteChannel, buffer );
                if ( dr == null )
                {
                    return null;
                }
                record.addNameRecord( dr );
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
            case NEOSTORE_COMMAND:
                return NeoStoreCommand.readCommand( neoStore, byteChannel, buffer );
            case NONE: return null;
            default:
                throw new IOException( "Unknown command type[" + commandType
                    + "]" );
        }
    }

    abstract boolean isCreated();

    abstract boolean isDeleted();
}