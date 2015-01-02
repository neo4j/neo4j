/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableCollection;

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.readAndFlip;

/**
 * Command implementations for all the commands that can be performed on a Neo
 * store.
 */
public abstract class Command extends XaCommand
{
    private final int keyHash;
    private final long key;
    private final Mode mode;

    /*
     * TODO: This is techdebt
     * This is used to control the order of how commands are applied, which is done because
     * we don't take read locks, and so the order or how we change things lowers the risk
     * of reading invalid state. This should be removed once eg. MVCC or read locks has been
     * implemented.
     */
    public enum Mode
    {
        CREATE,
        UPDATE,
        DELETE;

        public static Mode fromRecordState( boolean created, boolean inUse )
        {
            if ( !inUse )
            {
                return DELETE;
            }
            if ( created )
            {
                return CREATE;
            }
            return UPDATE;
        }

        public static Mode fromRecordState( AbstractBaseRecord record )
        {
            return fromRecordState( record.isCreated(), record.inUse() );
        }
    }

    Command( long key, Mode mode )
    {
        this.mode = mode;
        this.keyHash = (int) (( key >>> 32 ) ^ key );
        this.key = key;
    }

    public abstract void accept( CommandRecordVisitor visitor );

    @Override
    public int hashCode()
    {
        return keyHash;
    }

    // Force implementors to implement toString
    @Override
    public abstract String toString();

    long getKey()
    {
        return key;
    }

    Mode getMode()
    {
        return mode;
    }

    @Override
    public boolean equals( Object o )
    {
        return o != null && o.getClass().equals( getClass() ) && getKey() == ((Command) o).getKey();
    }

    private static void writePropertyBlock( LogBuffer buffer,
            PropertyBlock block ) throws IOException
    {
        byte blockSize = (byte) block.getSize();
        assert blockSize > 0 : blockSize + " is not a valid block size value";
        buffer.put( blockSize ); // 1
        long[] propBlockValues = block.getValueBlocks();
        for ( long propBlockValue : propBlockValues )
        {
            buffer.putLong( propBlockValue );
        }
        /*
         * For each block we need to keep its dynamic record chain if
         * it is just created. Deleted dynamic records are in the property
         * record and dynamic records are never modified. Also, they are
         * assigned as a whole, so just checking the first should be enough.
         */
        if ( block.isLight() )
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
        {
            writeDynamicRecord( buffer, record );
        }
    }

    static void writeDynamicRecord( LogBuffer buffer, DynamicRecord record )
        throws IOException
    {
        // id+type+in_use(byte)+nr_of_bytes(int)+next_block(long)
        if ( record.inUse() )
        {
            byte inUse = Record.IN_USE.byteValue();
            if ( record.isStartRecord() )
            {
                inUse |= Record.FIRST_IN_CHAIN.byteValue();
            }
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
        {
            return null;
        }
        byte blockSize = buffer.get(); // the size is stored in bytes // 1
        assert blockSize > 0 && blockSize % 8 == 0 : blockSize
                                                     + " is not a valid block size value";
        // Read in blocks
        if ( !readAndFlip( byteChannel, buffer, blockSize ) )
        {
            return null;
        }
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
        {
            return null;
        }

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
        {
            return false;
        }
        int numberOfRecords = buffer.getInt();
        assert numberOfRecords >= 0;
        while ( numberOfRecords-- > 0 )
        {
            DynamicRecord read = readDynamicRecord( byteChannel, buffer );
            if ( read == null )
            {
                return false;
            }
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
        {
            return null;
        }
        long id = buffer.getLong();
        assert id >= 0 && id <= ( 1l << 36 ) - 1 : id
                                                  + " is not a valid dynamic record id";
        int type = buffer.getInt();
        byte inUseFlag = buffer.get();
        boolean inUse = ( inUseFlag & Record.IN_USE.byteValue() ) != 0;

        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( inUse, type );
        if ( inUse )
        {
            record.setStartRecord( ( inUseFlag & Record.FIRST_IN_CHAIN.byteValue() ) != 0 );
            if ( !readAndFlip( byteChannel, buffer, 12 ) )
            {
                return null;
            }
            int nrOfBytes = buffer.getInt();
            assert nrOfBytes >= 0 && nrOfBytes < ( ( 1 << 24 ) - 1 ) : nrOfBytes
                                                                      + " is not valid for a number of bytes field of a dynamic record";
            long nextBlock = buffer.getLong();
            assert ( nextBlock >= 0 && nextBlock <= ( 1l << 36 - 1 ) )
                   || ( nextBlock == Record.NO_NEXT_BLOCK.intValue() ) : nextBlock
                                                                    + " is not valid for a next record field of a dynamic record";
            record.setNextBlock( nextBlock );
            if ( !readAndFlip( byteChannel, buffer, nrOfBytes ) )
            {
                return null;
            }
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
    private static final byte LABEL_KEY_COMMAND = (byte) 8;

    abstract void removeFromCache( CacheAccessBackDoor cacheAccess );

    public static class NodeCommand extends Command
    {
        private final NodeStore store;
        private final NodeRecord before;
        private final NodeRecord after;

        public NodeCommand( NodeStore store, NodeRecord before, NodeRecord after )
        {
            super( after.getId(), Mode.fromRecordState( after ) );
            this.store = store;
            this.before = before;
            this.after = after;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitNode( after );
        }

        @Override
        public String toString()
        {
            return beforeAndAfterToString( before, after );
        }

        @Override
        void removeFromCache( CacheAccessBackDoor cacheAccess )
        {
            cacheAccess.removeNodeFromCache( getKey() );
        }

        @Override
        public void execute()
        {
            store.updateRecord( after );

            // Dynamic Label Records
            Collection<DynamicRecord> toUpdate = new ArrayList<>( after.getDynamicLabelRecords() );
            addRemoved( toUpdate );
            store.updateDynamicLabelRecords( toUpdate );
        }

        private void addRemoved( Collection<DynamicRecord> toUpdate )
        {
            // the dynamic label records that exist in before, but not in after should be deleted.
            Set<Long> idsToRemove = new HashSet<>();
            for ( DynamicRecord record : before.getDynamicLabelRecords() )
            {
                idsToRemove.add( record.getId() );
            }
            for ( DynamicRecord record : after.getDynamicLabelRecords() )
            {
                idsToRemove.remove( record.getId() );
            }
            for ( long id : idsToRemove )
            {
                toUpdate.add( new DynamicRecord( id ) );
            }
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            buffer.put( NODE_COMMAND );
            buffer.putLong( after.getId() );

            writeNodeRecord( buffer, before );
            writeNodeRecord( buffer, after );
        }

        private void writeNodeRecord( LogBuffer buffer, NodeRecord record ) throws IOException
        {
            byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                    : Record.NOT_IN_USE.byteValue();
            buffer.put( inUse );
            if ( record.inUse() )
            {
                buffer.putLong( record.getNextRel() ).putLong( record.getNextProp() );

                // labels
                buffer.putLong( record.getLabelField() );
                writeDynamicRecords( buffer, record.getDynamicLabelRecords() );
            }
        }

        public static Command readFromFile( NeoStore neoStore, ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
        {
            if ( !readAndFlip( byteChannel, buffer, 8 ) )
            {
                return null;
            }
            long id = buffer.getLong();

            NodeRecord before = readNodeRecord( id, byteChannel, buffer );
            if ( before == null )
            {
                return null;
            }

            NodeRecord after = readNodeRecord( id, byteChannel, buffer );
            if ( after == null )
            {
                return null;
            }

            if ( !before.inUse() && after.inUse() )
            {
                after.setCreated();
            }

            return new NodeCommand( neoStore == null ? null : neoStore.getNodeStore(), before, after );
        }

        private static NodeRecord readNodeRecord( long id, ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
        {
            if ( !readAndFlip( byteChannel, buffer, 1 ) )
            {
                return null;
            }
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
                {
                    return null;
                }
                record = new NodeRecord( id, buffer.getLong(), buffer.getLong() );

                // labels
                long labelField = buffer.getLong();
                Collection<DynamicRecord> dynamicLabelRecords = new ArrayList<>();
                readDynamicRecords( byteChannel, buffer, dynamicLabelRecords, COLLECTION_DYNAMIC_RECORD_ADDER );
                record.setLabelField( labelField, dynamicLabelRecords );
            }
            else
            {
                record = new NodeRecord( id, Record.NO_NEXT_RELATIONSHIP.intValue(),
                        Record.NO_NEXT_PROPERTY.intValue() );
            }

            record.setInUse( inUse );
            return record;
        }

        public NodeRecord getBefore()
        {
            return before;
        }

        public NodeRecord getAfter()
        {
            return after;
        }
    }

    public static class RelationshipCommand extends Command
    {
        private final RelationshipRecord record;
        // before update stores the record as it looked before the command is executed
        private RelationshipRecord beforeUpdate;
        private final RelationshipStore store;

        public RelationshipCommand( RelationshipStore store, RelationshipRecord record )
        {
            super( record.getId(), Mode.fromRecordState( record ) );
            this.record = record;
            // the default (common) case is that the record to be written is complete and not from recovery or HA
            this.beforeUpdate = record;
            this.store = store;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitRelationship( record );
        }

        @Override
        public String toString()
        {
            return record.toString();
        }

        @Override
        void removeFromCache( CacheAccessBackDoor cacheAccess )
        {
            cacheAccess.removeRelationshipFromCache( getKey() );
            /*
             * If isRecovered() then beforeUpdate is the correct one UNLESS this is the second time this command
             * is executed, where it might have been actually written out to disk so the fields are already -1. So
             * we still need to check.
             * If !isRecovered() then beforeUpdate is the same as record, so we are still ok.
             * We don't check for !inUse() though because that is implicit in the call of this method.
             * The above is a hand waiving proof that the conditions that lead to the patchDeletedRelationshipNodes()
             * in the if below are the same as in RelationshipCommand.execute() so it should be safe.
             */
            if ( beforeUpdate.getFirstNode() != -1 || beforeUpdate.getSecondNode() != -1 )
            {
                cacheAccess.patchDeletedRelationshipNodes( getKey(), beforeUpdate.getFirstNode(),
                        beforeUpdate.getFirstNextRel(), beforeUpdate.getSecondNode(), beforeUpdate.getSecondNextRel() );
            }
            if ( record.getFirstNode() != -1 || record.getSecondNode() != -1 )
            {
                cacheAccess.removeNodeFromCache( record.getFirstNode() );
                cacheAccess.removeNodeFromCache( record.getSecondNode() );
            }
        }

        @Override
        public void execute()
        {
            if ( isRecovered() && !record.inUse() )
            {
                /*
                 * If read from a log (either on recovery or HA) then all the fields but for the Id are -1. If the
                 * record is deleted, then we'll need to invalidate the cache and patch the node's relationship chains.
                 * Therefore, we need to read the record from the store. This is not too expensive, since the window
                 * will be either in memory or will soon be anyway and we are just saving the write the trouble.
                 */
                beforeUpdate = store.forceGetRaw( record.getId() );
            }
            store.updateRecord( record );
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
                buffer.putLong( record.getFirstNode() )
                        .putLong( record.getSecondNode() )
                        .putInt( record.getType() )
                        .putLong( record.getFirstPrevRel() )
                        .putLong( record.getFirstNextRel() )
                        .putLong( record.getSecondPrevRel() )
                        .putLong( record.getSecondNextRel() )
                        .putLong( record.getNextProp() )
                        ;
            }
        }

        public static Command readFromFile( NeoStore neoStore,
            ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
        {
            if ( !readAndFlip( byteChannel, buffer, 9 ) )
            {
                return null;
            }
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
                {
                    return null;
                }
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

    public static class NeoStoreCommand extends Command
    {
        private final NeoStoreRecord record;
        private final NeoStore neoStore;

        public NeoStoreCommand( NeoStore neoStore, NeoStoreRecord record )
        {
            super( record.getId(), Mode.fromRecordState( record ) );
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
        public String toString()
        {
            return record.toString();
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

        public static Command readFromFile( NeoStore neoStore,
                ReadableByteChannel byteChannel, ByteBuffer buffer )
                throws IOException
        {
            if ( !readAndFlip( byteChannel, buffer, 8 ) )
            {
                return null;
            }
            long nextProp = buffer.getLong();
            NeoStoreRecord record = new NeoStoreRecord();
            record.setNextProp( nextProp );
            return new NeoStoreCommand( neoStore, record );
        }
    }

    public static class PropertyKeyTokenCommand extends Command
    {
        private final PropertyKeyTokenRecord record;
        private final PropertyKeyTokenStore store;

        public PropertyKeyTokenCommand( PropertyKeyTokenStore store,
                                        PropertyKeyTokenRecord record )
        {
            super( record.getId(), Mode.fromRecordState( record ) );
            this.record = record;
            this.store = store;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitPropertyKeyToken( record );
        }

        @Override
        public String toString()
        {
            return record.toString();
        }

        @Override
        void removeFromCache( CacheAccessBackDoor cacheAccess )
        {
            // no-op
        }

        @Override
        public void execute()
        {
            store.updateRecord( record );
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

        public static Command readFromFile( NeoStore neoStore, ReadableByteChannel byteChannel,
            ByteBuffer buffer ) throws IOException
        {
            // id+in_use(byte)+count(int)+key_blockId(int)
            if ( !readAndFlip( byteChannel, buffer, 13 ) )
            {
                return null;
            }
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
            PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( id );
            record.setInUse( inUse );
            record.setPropertyCount( buffer.getInt() );
            record.setNameId( buffer.getInt() );
            if ( !readDynamicRecords( byteChannel, buffer, record, PROPERTY_INDEX_DYNAMIC_RECORD_ADDER ) )
            {
                return null;
            }
            return new PropertyKeyTokenCommand( neoStore == null ? null : neoStore.getPropertyStore()
                .getPropertyKeyTokenStore(), record );
        }
    }

    private static final DynamicRecordAdder<PropertyKeyTokenRecord> PROPERTY_INDEX_DYNAMIC_RECORD_ADDER =
            new DynamicRecordAdder<PropertyKeyTokenRecord>()
    {
        @Override
        public void add( PropertyKeyTokenRecord target, DynamicRecord record )
        {
            target.addNameRecord( record );
        }
    };

    public static class PropertyCommand extends Command implements PropertyRecordChange
    {
        private final PropertyStore store;
        private final PropertyRecord before;
        private final PropertyRecord after;

        // TODO as optimization the deserialized key/values could be passed in here
        // so that the cost of deserializing them only applies in recovery/HA
        public PropertyCommand( PropertyStore store, PropertyRecord before, PropertyRecord after )
        {
            super( after.getId(), Mode.fromRecordState( after ) );
            this.store = store;
            this.before = before;
            this.after = after;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitProperty( after );
        }

        @Override
        public String toString()
        {
            return beforeAndAfterToString( before, after );
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
        public PropertyRecord getBefore()
        {
            return before;
        }

        @Override
        public PropertyRecord getAfter()
        {
            return after;
        }

        @Override
        public void execute()
        {
            store.updateRecord( after );
        }

        public long getNodeId()
        {
            return after.getNodeId();
        }

        public long getRelId()
        {
            return after.getRelId();
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            // COMMAND + ID
            buffer.put( PROP_COMMAND );
            buffer.putLong( getKey() ); // 8

            // BEFORE
            writeToFile( buffer, before );

            // AFTER
            writeToFile( buffer, after );
        }

        private void writeToFile( LogBuffer buffer, PropertyRecord record ) throws IOException
        {
            byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                    : Record.NOT_IN_USE.byteValue();
            if ( record.getRelId() != -1 )
            {
                // Here we add 2, i.e. set the second lsb.
                inUse += Record.REL_PROPERTY.byteValue();
            }
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

        public static Command readFromFile( NeoStore neoStore,
            ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
        {
            // ID
            if ( !readAndFlip( byteChannel, buffer, 8 ) )
            {
                return null;
            }
            long id = buffer.getLong(); // 8

            // BEFORE
            PropertyRecord before = readPropertyRecord( id, byteChannel, buffer );
            if ( before == null )
            {
                return null;
            }

            // AFTER
            PropertyRecord after = readPropertyRecord( id, byteChannel, buffer );
            if ( after == null )
            {
                return null;
            }

            return new PropertyCommand( neoStore == null ? null
                    : neoStore.getPropertyStore(), before, after );
        }

        private static PropertyRecord readPropertyRecord( long id, ReadableByteChannel byteChannel, ByteBuffer buffer )
                throws IOException
        {
            // in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
            // prev_prop_id(long)+next_prop_id(long)
            if ( !readAndFlip( byteChannel, buffer, 1 + 8 + 8 + 8 ) )
            {
                return null;
            }

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
            {
                return null;
            }
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
                record.addDeletedRecord( read );
            }

            if ( ( inUse && !record.inUse() ) || ( !inUse && record.inUse() ) )
            {
                throw new IllegalStateException( "Weird, inUse was read in as "
                                                 + inUse
                                                 + " but the record is "
                                                 + record );
            }
            return record;
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

    public static class RelationshipTypeTokenCommand extends Command
    {
        private final RelationshipTypeTokenRecord record;
        private final RelationshipTypeTokenStore store;

        public RelationshipTypeTokenCommand( RelationshipTypeTokenStore store,
                                             RelationshipTypeTokenRecord record )
        {
            super( record.getId(), Mode.fromRecordState( record ) );
            this.record = record;
            this.store = store;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitRelationshipTypeToken( record );
        }

        @Override
        public String toString()
        {
            return record.toString();
        }

        @Override
        void removeFromCache( CacheAccessBackDoor cacheAccess )
        {
            // no-op
        }

        @Override
        public void execute()
        {
            store.updateRecord( record );
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

        public static Command readFromFile( NeoStore neoStore,
            ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
        {
            // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
            if ( !readAndFlip( byteChannel, buffer, 13 ) )
            {
                return null;
            }
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
            RelationshipTypeTokenRecord record = new RelationshipTypeTokenRecord( id );
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
            return new RelationshipTypeTokenCommand(
                    neoStore == null ? null : neoStore.getRelationshipTypeStore(), record );
        }
    }

    static class LabelTokenCommand extends Command
    {
        private final LabelTokenRecord record;
        private final LabelTokenStore store;

        LabelTokenCommand( LabelTokenStore store,
                           LabelTokenRecord record )
        {
            super( record.getId(), Mode.fromRecordState( record ) );
            this.record = record;
            this.store = store;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitLabelToken( record );
        }

        @Override
        public String toString()
        {
            return record.toString();
        }

        @Override
        void removeFromCache( CacheAccessBackDoor cacheAccess )
        {
            // no-op
        }

        @Override
        public void execute()
        {
            store.updateRecord( record );
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
            byte inUse = record.inUse() ? Record.IN_USE.byteValue()
                : Record.NOT_IN_USE.byteValue();
            buffer.put( LABEL_KEY_COMMAND );
            buffer.putInt( record.getId() ).put( inUse ).putInt( record.getNameId() );
            writeDynamicRecords( buffer, record.getNameRecords() );
        }

        public static Command readFromFile( NeoStore neoStore,
            ReadableByteChannel byteChannel, ByteBuffer buffer )
            throws IOException
        {
            // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
            if ( !readAndFlip( byteChannel, buffer, 13 ) )
            {
                return null;
            }
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
            LabelTokenRecord record = new LabelTokenRecord( id );
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
            return new LabelTokenCommand(
                    neoStore == null ? null : neoStore.getLabelTokenStore(), record );
        }
    }

    static class SchemaRuleCommand extends Command
    {
        private final NeoStore neoStore;
        private final IndexingService indexes;
        private final SchemaStore store;
        private final Collection<DynamicRecord> recordsBefore;
        private final Collection<DynamicRecord> recordsAfter;
        private final SchemaRule schemaRule;

        private long txId;

        SchemaRuleCommand( NeoStore neoStore, SchemaStore store, IndexingService indexes,
                           Collection<DynamicRecord> recordsBefore, Collection<DynamicRecord> recordsAfter,
                           SchemaRule schemaRule, long txId )
        {
            super( first( recordsAfter ).getId(), Mode.fromRecordState( first( recordsAfter ) ) );
            this.neoStore = neoStore;
            this.indexes = indexes;
            this.store = store;
            this.recordsBefore = recordsBefore;
            this.recordsAfter = recordsAfter;
            this.schemaRule = schemaRule;
            this.txId = txId;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitSchemaRule( recordsAfter );
        }

        @Override
        public String toString()
        {
            if ( schemaRule != null )
            {
                return getMode() + ":" + schemaRule.toString();
            }
            return "SchemaRule" + recordsAfter;
        }

        @Override
        void removeFromCache( CacheAccessBackDoor cacheAccess )
        {
            cacheAccess.removeSchemaRuleFromCache( getKey() );
        }

        Collection<DynamicRecord> getRecordsAfter()
        {
            return unmodifiableCollection( recordsAfter );
        }

        @Override
        public void execute()
        {
            for ( DynamicRecord record : recordsAfter )
            {
                store.updateRecord( record );
            }

            if ( schemaRule instanceof IndexRule )
            {
                switch ( getMode() )
                {
                case UPDATE:
                    // Shouldn't we be more clear about that we are waiting for an index to come online here?
                    // right now we just assume that an update to index records means wait for it to be online.
                    if ( ((IndexRule) schemaRule).isConstraintIndex() )
                    {
                        try
                        {
                            indexes.activateIndex( schemaRule.getId() );
                        }
                        catch ( IndexNotFoundKernelException | IndexActivationFailedKernelException |
                                IndexPopulationFailedKernelException e )
                        {
                            throw new IllegalStateException( "Unable to enable constraint, backing index is not online.", e );
                        }
                    }
                    break;
                case CREATE:
                    indexes.createIndex( (IndexRule) schemaRule );
                    break;
                case DELETE:
                    indexes.dropIndex( (IndexRule)schemaRule );
                    break;
                default:
                    throw new IllegalStateException( getMode().name() );
                }
            }

            if( schemaRule instanceof UniquenessConstraintRule )
            {
                switch ( getMode() )
                {
                    case UPDATE:
                    case CREATE:
                        neoStore.setLatestConstraintIntroducingTx( txId );
                        break;
                    case DELETE:
                        break;
                    default:
                        throw new IllegalStateException( getMode().name() );
                }
            }
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            buffer.put( SCHEMA_RULE_COMMAND );
            writeDynamicRecords( buffer, recordsBefore );
            writeDynamicRecords( buffer, recordsAfter );
            buffer.put( first( recordsAfter ).isCreated() ? (byte) 1 : 0);
            buffer.putLong( txId );
        }

        public SchemaRule getSchemaRule()
        {
            return schemaRule;
        }

        public long getTxId()
        {
            return txId;
        }

        public void setTxId( long txId )
        {
            this.txId = txId;
        }

        static Command readFromFile( NeoStore neoStore, IndexingService indexes, ReadableByteChannel byteChannel,
                ByteBuffer buffer ) throws IOException
        {
            Collection<DynamicRecord> recordsBefore = new ArrayList<>();
            readDynamicRecords( byteChannel, buffer, recordsBefore, COLLECTION_DYNAMIC_RECORD_ADDER );

            Collection<DynamicRecord> recordsAfter = new ArrayList<>();
            readDynamicRecords( byteChannel, buffer, recordsAfter, COLLECTION_DYNAMIC_RECORD_ADDER );

            if ( !readAndFlip( byteChannel, buffer, 1 ) )
            {
                throw new IllegalStateException( "Missing SchemaRule.isCreated flag in deserialization" );
            }

            byte isCreated = buffer.get();
            if ( 1 == isCreated )
            {
                for ( DynamicRecord record : recordsAfter )
                {
                    record.setCreated();
                }
            }

            if ( !readAndFlip( byteChannel, buffer, 8 ) )
            {
                throw new IllegalStateException( "Missing SchemaRule.txId in deserialization" );
            }

            long txId = buffer.getLong();

            SchemaRule rule = first( recordsAfter ).inUse() ?
                    readSchemaRule( recordsAfter ) :
                    readSchemaRule( recordsBefore );

            return new SchemaRuleCommand( neoStore, neoStore != null ? neoStore.getSchemaStore() : null,
                    indexes, recordsBefore, recordsAfter, rule, txId );
        }

        private static SchemaRule readSchemaRule( Collection<DynamicRecord> recordsBefore )
        {
            assert first(recordsBefore).inUse() : "Asked to deserialize schema records that were not in use.";

            SchemaRule rule;
            ByteBuffer deserialized = AbstractDynamicStore.concatData( recordsBefore, new byte[100] );
            try
            {
                rule = SchemaRule.Kind.deserialize( first( recordsBefore ).getId(), deserialized );
            }
            catch ( MalformedSchemaRuleException e )
            {
                // TODO This is bad. We should probably just shut down if that happens
                throw launderedException( e );
            }
            return rule;
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

    public static Command readCommand( NeoStore neoStore, IndexingService indexes, ReadableByteChannel byteChannel,
        ByteBuffer buffer ) throws IOException
    {
        if ( !readAndFlip( byteChannel, buffer, 1 ) )
        {
            return null;
        }
        byte commandType = buffer.get();
        switch ( commandType )
        {
            case NODE_COMMAND:
                return NodeCommand.readFromFile( neoStore, byteChannel, buffer );
            case PROP_COMMAND:
                return PropertyCommand.readFromFile( neoStore, byteChannel, buffer );
            case PROP_INDEX_COMMAND:
                return PropertyKeyTokenCommand.readFromFile( neoStore, byteChannel, buffer );
            case REL_COMMAND:
                return RelationshipCommand.readFromFile( neoStore, byteChannel, buffer );
            case REL_TYPE_COMMAND:
                return RelationshipTypeTokenCommand.readFromFile( neoStore, byteChannel, buffer );
            case LABEL_KEY_COMMAND:
                return LabelTokenCommand.readFromFile( neoStore, byteChannel, buffer );
            case NEOSTORE_COMMAND:
                return NeoStoreCommand.readFromFile( neoStore, byteChannel, buffer );
            case SCHEMA_RULE_COMMAND:
                return SchemaRuleCommand.readFromFile( neoStore, indexes, byteChannel, buffer );
            case NONE: return null;
            default:
                throw new IOException( "Unknown command type[" + commandType + "]" );
        }
    }

    static String beforeAndAfterToString( AbstractBaseRecord before, AbstractBaseRecord after )
    {
        return format( "%n  -%s%n  +%s", before, after );
    }
}
