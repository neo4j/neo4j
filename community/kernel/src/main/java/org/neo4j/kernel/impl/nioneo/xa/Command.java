/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.readAndFlip;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
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
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

/**
 * Command implementations for all the commands that can be performed on a Neo
 * store.
 */
public abstract class Command extends XaCommand
{
    private final int keyHash;
    private final AbstractBaseRecord record;

    Command( AbstractBaseRecord record )
    {
        this.record = record;
        long key = getKey();
        this.keyHash = (int) (( key >>> 32 ) ^ key );
    }
    
    public abstract void accept( CommandRecordVisitor visitor );
    
    @Override
    // Makes this method publically visible
    public void setRecovered()
    {
        super.setRecovered();
    }

    long getKey()
    {
        return record.getLongId();
    }

    @Override
    public int hashCode()
    {
        return keyHash;
    }
    
    @Override
    public String toString()
    {
        return record.toString();
    }

    boolean isCreated()
    {
        return record.isCreated();
    }

    boolean isDeleted()
    {
        return !record.inUse();
    }
    
    @Override
    public boolean equals( Object o )
    {
        if ( o == null || !(o.getClass().equals( getClass() )) )
        {
            return false;
        }
        return getKey() == ((Command) o).getKey();
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
            writeDynamicRecords( buffer, block.getValueRecords() );
        }
    }
    
    static void writeDynamicRecords( LogBuffer buffer, Collection<DynamicRecord> records ) throws IOException
    {
        buffer.putInt( records.size() ); // 4
        for ( DynamicRecord record : records )
            writeDynamicRecord( buffer, record );
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
        if ( !readAndFlip( byteChannel, buffer, 1 ) )
            return null;
        byte blockSize = buffer.get(); // the size is stored in bytes // 1
        assert blockSize > 0 && blockSize % 8 == 0 : blockSize
                                                     + " is not a valid block size value";
        // Read in blocks
        if ( !readAndFlip( byteChannel, buffer, blockSize ) )
            return null;
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
        if ( !readDynamicRecords( byteChannel, buffer, toReturn, PROPERTY_BLOCK_DYNAMIC_RECORD_ADDER ) )
            return null;

        // TODO we had this assertion before, necessary?
//            assert toReturn.getValueRecords().size() == noOfDynRecs : "read in "
//                                                                      + toReturn.getValueRecords().size()
//                                                                      + " instead of the proper "
//                                                                      + noOfDynRecs;
        return toReturn;
    }
    
    private static final DynamicRecordAdder<PropertyBlock> PROPERTY_BLOCK_DYNAMIC_RECORD_ADDER =
            new DynamicRecordAdder<PropertyBlock>()
    {
        @Override
        public void add( PropertyBlock target, DynamicRecord record )
        {
            record.setCreated();
            target.addValueRecord( record );
        }
    };
    
    static <T> boolean readDynamicRecords( ReadableByteChannel byteChannel, ByteBuffer buffer,
            T target, DynamicRecordAdder<T> adder ) throws IOException
    {
        if ( !readAndFlip( byteChannel, buffer, 4 ) )
            return false;
        int numberOfRecords = buffer.getInt();
        assert numberOfRecords >= 0;
        while ( numberOfRecords-- > 0 )
        {
            DynamicRecord read = readDynamicRecord( byteChannel, buffer );
            if ( read == null )
                return false;
            adder.add( target, read );
        }
        return true;
    }
    
    private interface DynamicRecordAdder<T>
    {
        void add( T target, DynamicRecord record );
    }

    static DynamicRecord readDynamicRecord( ReadableByteChannel byteChannel,
        ByteBuffer buffer ) throws IOException
    {
        // id+type+in_use(byte)+nr_of_bytes(int)+next_block(long)
        if ( !readAndFlip( byteChannel, buffer, 13 ) )
            return null;
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
            if ( !readAndFlip( byteChannel, buffer, 12 ) )
                return null;
            int nrOfBytes = buffer.getInt();
            assert nrOfBytes >= 0 && nrOfBytes < ( ( 1 << 24 ) - 1 ) : nrOfBytes
                                                                      + " is not valid for a number of bytes field of a dynamic record";
            long nextBlock = buffer.getLong();
            assert ( nextBlock >= 0 && nextBlock <= ( 1l << 36 - 1 ) )
                   || ( nextBlock == Record.NO_NEXT_BLOCK.intValue() ) : nextBlock
                                                                    + " is not valid for a next record field of a dynamic record";
            record.setNextBlock( nextBlock );
            if ( !readAndFlip( byteChannel, buffer, nrOfBytes ) )
                return null;
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
    private static final byte SCHEMA_RULE_COMMAND = (byte) 7;

    abstract void removeFromCache( CacheAccessBackDoor cacheAccess );

    static class NodeCommand extends Command
    {
        private final NodeRecord record;
        private final NodeStore store;

        NodeCommand( NodeStore store, NodeRecord record )
        {
            super( record );
            this.record = record;
            this.store = store;
        }
        
        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitNode( record );
        }

        @Override
        void removeFromCache( CacheAccessBackDoor cacheAccess )
        {
            cacheAccess.removeNodeFromCache( getKey() );
        }

        @Override
        public void execute()
        {
            if ( isRecovered() )
            {
                store.updateRecord( record, true );
            }
            else
            {
                store.updateRecord( record );
            }
            
            // Dynamic labels
            store.updateDynamicLabelRecords( record.getDynamicLabelRecords() );
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
                buffer.putLong( record.getNextRel() ).putLong( record.getNextProp() );
                
                // labels
                buffer.putLong( record.getLabelField() );
                writeDynamicRecords( buffer, record.getDynamicLabelRecords() );
            }
        }

        public static Command readSpecificCommand( NeoStore neoStore,
            ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
        {
            if ( !readAndFlip( byteChannel, buffer, 9 ) )
                return null;
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
                if ( !readAndFlip( byteChannel, buffer, 8*3 ) )
                    return null;
                record = new NodeRecord( id, buffer.getLong(), buffer.getLong() );
                
                // labels
                long labelField = buffer.getLong();
                Collection<DynamicRecord> dynamicLabelRecords = new ArrayList<DynamicRecord>();
                readDynamicRecords( byteChannel, buffer, dynamicLabelRecords, COLLECTION_DYNAMIC_RECORD_ADDER );
                record.setLabelField( labelField, dynamicLabelRecords );
            }
            else
                record = new NodeRecord( id, Record.NO_NEXT_RELATIONSHIP.intValue(),
                        Record.NO_NEXT_PROPERTY.intValue() );
            record.setInUse( inUse );
            return new NodeCommand( neoStore == null ? null : neoStore.getNodeStore(), record );
        }
    }

    static class RelationshipCommand extends Command
    {
        private final RelationshipRecord record;
        private final RelationshipStore store;

        RelationshipCommand( RelationshipStore store, RelationshipRecord record )
        {
            super( record );
            this.record = record;
            this.store = store;
        }
        
        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitRelationship( record );
        }

        @Override
        void removeFromCache( CacheAccessBackDoor cacheAccess )
        {
            cacheAccess.removeRelationshipFromCache( getKey() );
            cacheAccess.patchDeletedRelationshipNodes( getKey(), record.getFirstNode(), record.getFirstNextRel(),
                    record.getSecondNode(), record.getSecondNextRel() );
            if ( this.getFirstNode() != -1 || this.getSecondNode() != -1 )
            {
                cacheAccess.removeNodeFromCache( this.getFirstNode() );
                cacheAccess.removeNodeFromCache( this.getSecondNode() );
            }
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
                store.updateRecord( record, true );
            }
            else
            {
                store.updateRecord( record );
            }
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

        public static Command readSpecificCommand( NeoStore neoStore,
            ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
        {
            if ( !readAndFlip( byteChannel, buffer, 9 ) )
                return null;
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
                if ( !readAndFlip( byteChannel, buffer, 60 ) )
                    return null;
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
    }
    
    static class NeoStoreCommand extends Command
    {
        private final NeoStoreRecord record;
        private final NeoStore neoStore;

        NeoStoreCommand( NeoStore neoStore, NeoStoreRecord record )
        {
            super( record );
            this.neoStore = neoStore;
            this.record = record;
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
        void removeFromCache( CacheAccessBackDoor cacheAccess )
        {
            // no-op
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            buffer.put( NEOSTORE_COMMAND ).putLong( record.getNextProp() );
        }

        public static Command readSpecificCommand( NeoStore neoStore,
                ReadableByteChannel byteChannel, ByteBuffer buffer )
                throws IOException
        {
            if ( !readAndFlip( byteChannel, buffer, 8 ) )
                return null;
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
            super( record );
            this.record = record;
            this.store = store;
        }
        
        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitPropertyIndex( record );
        }

        @Override
        void removeFromCache( CacheAccessBackDoor cacheAccess )
        {
            // no-op
        }

        @Override
        public void execute()
        {
            if ( isRecovered() )
            {
                store.updateRecord( record, true );
            }
            else
            {
                store.updateRecord( record );
            }
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
                writeDynamicRecords( buffer, record.getNameRecords() );
            }
        }

        public static Command readSpecificCommand( NeoStore neoStore, ReadableByteChannel byteChannel,
            ByteBuffer buffer ) throws IOException
        {
            // id+in_use(byte)+count(int)+key_blockId(int)
            if ( !readAndFlip( byteChannel, buffer, 13 ) )
                return null;
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
            if ( !readDynamicRecords( byteChannel, buffer, record, PROPERTY_INDEX_DYNAMIC_RECORD_ADDER ) )
                return null;
            return new PropertyIndexCommand( neoStore == null ? null : neoStore.getPropertyStore()
                .getIndexStore(), record );
        }
    }
    
    private static final DynamicRecordAdder<PropertyIndexRecord> PROPERTY_INDEX_DYNAMIC_RECORD_ADDER =
            new DynamicRecordAdder<PropertyIndexRecord>()
    {
        @Override
        public void add( PropertyIndexRecord target, DynamicRecord record )
        {
            target.addNameRecord( record );
        }
    };

    static class PropertyCommand extends Command
    {
        private final PropertyRecord record;
        private final PropertyStore store;

        PropertyCommand( PropertyStore store, PropertyRecord record )
        {
            super( record );
            this.record = record;
            this.store = store;
        }
        
        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitProperty( record );
        }

        @Override
        void removeFromCache( CacheAccessBackDoor cacheAccess )
        {
            long nodeId = this.getNodeId();
            long relId = this.getRelId();
            if ( nodeId != -1 )
            {
                cacheAccess.removeNodeFromCache( nodeId );
            }
            else if ( relId != -1 )
            {
                cacheAccess.removeRelationshipFromCache( relId );
            }
        }

        @Override
        public void execute()
        {
            if ( isRecovered() )
            {
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
            writeDynamicRecords( buffer, record.getDeletedRecords() );
        }

        public static Command readSpecificCommand( NeoStore neoStore,
            ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
        {
            // id+in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
            // prev_prop_id(long)+next_prop_id(long)
            if ( !readAndFlip( byteChannel, buffer, 8 + 1 + 8 + 8 + 8 ) )
                return null;

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
            if ( !readAndFlip( byteChannel, buffer, 1 ) )
                return null;
            int nrPropBlocks = buffer.get();
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
            
            if ( !readDynamicRecords( byteChannel, buffer, record, PROPERTY_DELETED_DYNAMIC_RECORD_ADDER ) )
                return null;
            
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
    }
    
    private static final DynamicRecordAdder<PropertyRecord> PROPERTY_DELETED_DYNAMIC_RECORD_ADDER =
            new DynamicRecordAdder<PropertyRecord>()
    {
        @Override
        public void add( PropertyRecord target, DynamicRecord record )
        {
            assert !record.inUse() : record + " is kinda weird";
            target.addDeletedRecord( record );
        }
    };

    static class RelationshipTypeCommand extends Command
    {
        private final RelationshipTypeRecord record;
        private final RelationshipTypeStore store;

        RelationshipTypeCommand( RelationshipTypeStore store,
            RelationshipTypeRecord record )
        {
            super( record );
            this.record = record;
            this.store = store;
        }
        
        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitRelationshipType( record );
        }

        @Override
        void removeFromCache( CacheAccessBackDoor cacheAccess )
        {
            // no-op
        }

        @Override
        public void execute()
        {
            if ( isRecovered() )
            {
                store.updateRecord( record, true );
            }
            else
            {
                store.updateRecord( record );
            }
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
            byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
            buffer.put( REL_TYPE_COMMAND );
            buffer.putInt( record.getId() ).put( inUse ).putInt( record.getNameId() );
            writeDynamicRecords( buffer, record.getNameRecords() );
        }

        public static Command readSpecificCommand( NeoStore neoStore,
            ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
        {
            // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
            if ( !readAndFlip( byteChannel, buffer, 13 ) )
                return null;
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
    }
    
    static class SchemaRuleCommand extends Command
    {
        private final SchemaStore store;
        private final Collection<DynamicRecord> records;
        private final SchemaRule schemaRule;

        SchemaRuleCommand( SchemaStore store, Collection<DynamicRecord> records, SchemaRule schemaRule )
        {
            super( first( records ) );
            this.store = store;
            this.records = records;
            this.schemaRule = schemaRule;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        void removeFromCache( CacheAccessBackDoor cacheAccess )
        {
            cacheAccess.removeSchemaRuleFromCache( getKey() );
        }

        @Override
        public void execute()
        {
            for ( DynamicRecord record : records )
                store.updateRecord( record );
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            buffer.put( SCHEMA_RULE_COMMAND );
            writeDynamicRecords( buffer, records );
        }
        
        public SchemaRule getSchemaRule()
        {
            return schemaRule;
        }
        
        @Override
        public String toString()
        {
            return "SchemaRule" + records;
        }

        static Command readSpecificCommand( NeoStore neoStore, ReadableByteChannel byteChannel, ByteBuffer buffer )
                throws IOException
        {
            Collection<DynamicRecord> records = new ArrayList<DynamicRecord>();
            readDynamicRecords( byteChannel, buffer, records, COLLECTION_DYNAMIC_RECORD_ADDER );
            ByteBuffer deserialized = AbstractDynamicStore.concatData( records, new byte[100] );
            return new SchemaRuleCommand( neoStore.getSchemaStore(), records,
                    SchemaRule.Kind.deserialize( first( records ).getId(), deserialized ) );
        }
    }
    
    private static final DynamicRecordAdder<Collection<DynamicRecord>> COLLECTION_DYNAMIC_RECORD_ADDER =
            new DynamicRecordAdder<Collection<DynamicRecord>>()
    {
        @Override
        public void add( Collection<DynamicRecord> target, DynamicRecord record )
        {
            target.add( record );
        }
    };

    public static Command readCommand( NeoStore neoStore, ReadableByteChannel byteChannel,
        ByteBuffer buffer ) throws IOException
    {
        if ( !readAndFlip( byteChannel, buffer, 1 ) )
            return null;
        byte commandType = buffer.get();
        switch ( commandType )
        {
            case NODE_COMMAND:
                return NodeCommand.readSpecificCommand( neoStore, byteChannel, buffer );
            case PROP_COMMAND:
                return PropertyCommand.readSpecificCommand( neoStore, byteChannel,
                    buffer );
            case PROP_INDEX_COMMAND:
                return PropertyIndexCommand.readSpecificCommand( neoStore, byteChannel,
                    buffer );
            case REL_COMMAND:
                return RelationshipCommand.readSpecificCommand( neoStore, byteChannel,
                    buffer );
            case REL_TYPE_COMMAND:
                return RelationshipTypeCommand.readSpecificCommand( neoStore,
                    byteChannel, buffer );
            case NEOSTORE_COMMAND:
                return NeoStoreCommand.readSpecificCommand( neoStore, byteChannel, buffer );
            case SCHEMA_RULE_COMMAND:
                return SchemaRuleCommand.readSpecificCommand( neoStore, byteChannel, buffer );
            case NONE: return null;
            default:
                throw new IOException( "Unknown command type[" + commandType
                    + "]" );
        }
    }
}
