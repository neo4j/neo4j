/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.recordstorage;

import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.recordstorage.CommandReading.DynamicRecordAdder;
import org.neo4j.internal.recordstorage.legacy.IndexCommand;
import org.neo4j.internal.recordstorage.legacy.IndexCommand.AddNodeCommand;
import org.neo4j.internal.recordstorage.legacy.IndexCommand.AddRelationshipCommand;
import org.neo4j.internal.recordstorage.legacy.IndexCommand.CreateCommand;
import org.neo4j.internal.recordstorage.legacy.IndexCommand.RemoveCommand;
import org.neo4j.internal.recordstorage.legacy.IndexDefineCommand;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.storemigration.legacy.SchemaRuleSerialization35;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.internal.helpers.Numbers.unsignedShortToInt;
import static org.neo4j.internal.recordstorage.CommandReading.COLLECTION_DYNAMIC_RECORD_ADDER;
import static org.neo4j.internal.recordstorage.CommandReading.PROPERTY_BLOCK_DYNAMIC_RECORD_ADDER;
import static org.neo4j.internal.recordstorage.CommandReading.PROPERTY_DELETED_DYNAMIC_RECORD_ADDER;
import static org.neo4j.internal.recordstorage.CommandReading.PROPERTY_INDEX_DYNAMIC_RECORD_ADDER;
import static org.neo4j.io.fs.IoPrimitiveUtils.read2bLengthAndString;
import static org.neo4j.io.fs.IoPrimitiveUtils.read2bMap;
import static org.neo4j.io.fs.IoPrimitiveUtils.read3bLengthAndString;
import static org.neo4j.util.Bits.bitFlag;

class LogCommandSerializationV3_0_10 extends LogCommandSerialization
{
    static LogCommandSerializationV3_0_10 INSTANCE = new LogCommandSerializationV3_0_10();

    @Override
    KernelVersion version()
    {
        return KernelVersion.V2_3;
    }

    @Override
    protected Command readNodeCommand( ReadableChannel channel ) throws IOException
    {
        long id = channel.getLong();
        NodeRecord before = readNodeRecord( id, channel );
        if ( before == null )
        {
            return null;
        }
        NodeRecord after = readNodeRecord( id, channel );
        if ( after == null )
        {
            return null;
        }
        if ( !before.inUse() && after.inUse() )
        {
            after.setCreated();
        }
        return new Command.NodeCommand( this, before, after );
    }

    @Override
    protected Command readRelationshipCommand( ReadableChannel channel ) throws IOException
    {
        long id = channel.getLong();

        RelationshipRecord before = readRelationshipRecord( id, channel );
        if ( before == null )
        {
            return null;
        }

        RelationshipRecord after = readRelationshipRecord( id, channel );
        if ( after == null )
        {
            return null;
        }

        if ( !before.inUse() && after.inUse() )
        {
            after.setCreated();
        }
        return new Command.RelationshipCommand( this, before, after );
    }

    @Override
    protected Command readPropertyCommand( ReadableChannel channel ) throws IOException
    {
        // ID
        long id = channel.getLong(); // 8
        // BEFORE
        PropertyRecord before = readPropertyRecord( id, channel );
        if ( before == null )
        {
            return null;
        }
        // AFTER
        PropertyRecord after = readPropertyRecord( id, channel );
        if ( after == null )
        {
            return null;
        }
        return new Command.PropertyCommand( this, before, after );
    }

    @Override
    protected Command readRelationshipGroupCommand( ReadableChannel channel ) throws IOException
    {
        long id = channel.getLong();
        RelationshipGroupRecord before = readRelationshipGroupRecord( id, channel );
        RelationshipGroupRecord after = readRelationshipGroupRecord( id, channel );
        return new Command.RelationshipGroupCommand( this, before, after );
    }

    private RelationshipGroupRecord readRelationshipGroupRecord( long id, ReadableChannel channel )
            throws IOException
    {
        int flags = channel.get() & 0xFF;
        boolean inUse = bitFlag( flags, Record.IN_USE.byteValue() );
        boolean requireSecondaryUnit = bitFlag( flags, Record.REQUIRE_SECONDARY_UNIT );
        boolean hasSecondaryUnit = bitFlag( flags, Record.HAS_SECONDARY_UNIT );
        boolean usesFixedReferenceFormat = bitFlag( flags, Record.USES_FIXED_REFERENCE_FORMAT );

        int type = unsignedShortToInt( channel.getShort() );
        long next = channel.getLong();
        long firstOut = channel.getLong();
        long firstIn = channel.getLong();
        long firstLoop = channel.getLong();
        long owningNode = channel.getLong();
        RelationshipGroupRecord record = new RelationshipGroupRecord( id ).initialize( inUse, type, firstOut, firstIn, firstLoop, owningNode, next );
        record.setRequiresSecondaryUnit( requireSecondaryUnit );
        if ( hasSecondaryUnit )
        {
            record.setSecondaryUnitIdOnLoad( channel.getLong() );
        }
        record.setUseFixedReferences( usesFixedReferenceFormat );
        return record;
    }

    @Override
    protected Command readRelationshipTypeTokenCommand( ReadableChannel channel ) throws IOException
    {
        int id = channel.getInt();
        RelationshipTypeTokenRecord before = readRelationshipTypeTokenRecord( id, channel );
        if ( before == null )
        {
            return null;
        }

        RelationshipTypeTokenRecord after = readRelationshipTypeTokenRecord( id, channel );
        if ( after == null )
        {
            return null;
        }

        return new Command.RelationshipTypeTokenCommand( this, before, after );
    }

    private RelationshipTypeTokenRecord readRelationshipTypeTokenRecord( int id, ReadableChannel channel )
            throws IOException
    {
        // in_use(byte)+type_blockId(int)+nr_type_records(int)
        byte inUseFlag = channel.get();
        boolean inUse = false;
        if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE.byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        RelationshipTypeTokenRecord record = new RelationshipTypeTokenRecord( id );
        record.setInUse( inUse );
        record.setNameId( channel.getInt() );
        int nrTypeRecords = channel.getInt();
        for ( int i = 0; i < nrTypeRecords; i++ )
        {
            DynamicRecord dr = readDynamicRecord( channel );
            if ( dr == null )
            {
                return null;
            }
            record.addNameRecord( dr );
        }
        return record;
    }

    @Override
    protected Command readLabelTokenCommand( ReadableChannel channel ) throws IOException
    {
        int id = channel.getInt();
        LabelTokenRecord before = readLabelTokenRecord( id, channel );
        if ( before == null )
        {
            return null;
        }

        LabelTokenRecord after = readLabelTokenRecord( id, channel );
        if ( after == null )
        {
            return null;
        }

        return new Command.LabelTokenCommand( this, before, after );
    }

    private LabelTokenRecord readLabelTokenRecord( int id, ReadableChannel channel ) throws IOException
    {
        // in_use(byte)+type_blockId(int)+nr_type_records(int)
        byte inUseFlag = channel.get();
        boolean inUse = false;
        if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE.byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        LabelTokenRecord record = new LabelTokenRecord( id );
        record.setInUse( inUse );
        record.setNameId( channel.getInt() );
        int nrTypeRecords = channel.getInt();
        for ( int i = 0; i < nrTypeRecords; i++ )
        {
            DynamicRecord dr = readDynamicRecord( channel );
            if ( dr == null )
            {
                return null;
            }
            record.addNameRecord( dr );
        }
        return record;
    }

    @Override
    protected Command readPropertyKeyTokenCommand( ReadableChannel channel ) throws IOException
    {
        int id = channel.getInt();
        PropertyKeyTokenRecord before = readPropertyKeyTokenRecord( id, channel );
        if ( before == null )
        {
            return null;
        }

        PropertyKeyTokenRecord after = readPropertyKeyTokenRecord( id, channel );
        if ( after == null )
        {
            return null;
        }

        return new Command.PropertyKeyTokenCommand( this, before, after );
    }

    private PropertyKeyTokenRecord readPropertyKeyTokenRecord( int id, ReadableChannel channel ) throws IOException
    {
        // in_use(byte)+count(int)+key_blockId(int)
        byte inUseFlag = channel.get();
        boolean inUse = false;
        if ( (inUseFlag & Record.IN_USE.byteValue()) == Record.IN_USE.byteValue() )
        {
            inUse = true;
        }
        else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
        {
            throw new IOException( "Illegal in use flag: " + inUseFlag );
        }
        PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( id );
        record.setInUse( inUse );
        record.setPropertyCount( channel.getInt() );
        record.setNameId( channel.getInt() );
        if ( readDynamicRecords( channel, record, PROPERTY_INDEX_DYNAMIC_RECORD_ADDER ) == -1 )
        {
            return null;
        }
        return record;
    }

    @Override
    protected Command readLegacySchemaRuleCommand( ReadableChannel channel ) throws IOException
    {
        Collection<DynamicRecord> recordsBefore = new ArrayList<>();
        readDynamicRecords( channel, recordsBefore, COLLECTION_DYNAMIC_RECORD_ADDER );
        Collection<DynamicRecord> recordsAfter = new ArrayList<>();
        readDynamicRecords( channel, recordsAfter, COLLECTION_DYNAMIC_RECORD_ADDER );
        byte isCreated = channel.get();
        if ( 1 == isCreated )
        {
            for ( DynamicRecord record : recordsAfter )
            {
                record.setCreated();
            }
        }
        SchemaRule rule = Iterables.first( recordsAfter ).inUse()
                          ? readSchemaRule( recordsAfter )
                          : readSchemaRule( recordsBefore );
        return new Command.SchemaRuleCommand( this, new SchemaRecord( 0 ), new SchemaRecord( 0 ), rule );
    }

    @Override
    protected Command readNeoStoreCommand( ReadableChannel channel ) throws IOException
    {
        NeoStoreRecord before = readNeoStoreRecord( channel );
        NeoStoreRecord after = readNeoStoreRecord( channel );
        return new Command.NeoStoreCommand( this, before, after );
    }

    private NeoStoreRecord readNeoStoreRecord( ReadableChannel channel ) throws IOException
    {
        long nextProp = channel.getLong();
        NeoStoreRecord record = new NeoStoreRecord();
        record.setNextProp( nextProp );
        return record;
    }

    private NodeRecord readNodeRecord( long id, ReadableChannel channel ) throws IOException
    {
        byte flags = channel.get();
        boolean inUse = bitFlag( flags, Record.IN_USE.byteValue() );
        boolean isCreated = bitFlag( flags, Record.CREATED_IN_TX );
        boolean requiresSecondaryUnit = bitFlag( flags, Record.REQUIRE_SECONDARY_UNIT );
        boolean hasSecondaryUnit = bitFlag( flags, Record.HAS_SECONDARY_UNIT );
        boolean usesFixedReferenceFormat = bitFlag( flags, Record.USES_FIXED_REFERENCE_FORMAT );

        NodeRecord record;
        Collection<DynamicRecord> dynamicLabelRecords = new ArrayList<>();
        long labelField = Record.NO_LABELS_FIELD.intValue();
        if ( inUse )
        {
            boolean dense = channel.get() == 1;
            long nextRel = channel.getLong();
            long nextProp = channel.getLong();
            record = new NodeRecord( id ).initialize( false, nextProp, dense, nextRel, 0 );
            // labels
            labelField = channel.getLong();
            record.setRequiresSecondaryUnit( requiresSecondaryUnit );
            if ( hasSecondaryUnit )
            {
                record.setSecondaryUnitIdOnLoad( channel.getLong() );
            }
            record.setUseFixedReferences( usesFixedReferenceFormat );
        }
        else
        {
            record = new NodeRecord( id );
        }
        readDynamicRecords( channel, dynamicLabelRecords, COLLECTION_DYNAMIC_RECORD_ADDER );
        record.setLabelField( labelField, dynamicLabelRecords );
        record.setInUse( inUse );
        if ( isCreated )
        {
            record.setCreated();
        }
        return record;
    }

    private RelationshipRecord readRelationshipRecord( long id, ReadableChannel channel ) throws IOException
    {
        byte flags = channel.get();
        boolean inUse = bitFlag( flags, Record.IN_USE.byteValue() );
        boolean requiresSecondaryUnit = bitFlag( flags, Record.REQUIRE_SECONDARY_UNIT );
        boolean hasSecondaryUnit = bitFlag( flags, Record.HAS_SECONDARY_UNIT );
        boolean usesFixedReferenceFormat = bitFlag( flags, Record.USES_FIXED_REFERENCE_FORMAT );

        RelationshipRecord record;
        if ( inUse )
        {
            record = new RelationshipRecord( id );
            record.setLinks( channel.getLong(), channel.getLong(), channel.getInt() );
            record.setInUse( true );
            record.setRequiresSecondaryUnit( requiresSecondaryUnit );
            record.setFirstPrevRel( channel.getLong() );
            record.setFirstNextRel( channel.getLong() );
            record.setSecondPrevRel( channel.getLong() );
            record.setSecondNextRel( channel.getLong() );
            record.setNextProp( channel.getLong() );
            byte extraByte = channel.get();
            record.setFirstInFirstChain( (extraByte & 0x1) > 0 );
            record.setFirstInSecondChain( (extraByte & 0x2) > 0 );
            if ( hasSecondaryUnit )
            {
                record.setSecondaryUnitIdOnLoad( channel.getLong() );
            }
            record.setUseFixedReferences( usesFixedReferenceFormat );
        }
        else
        {
            record = new RelationshipRecord( id );
            record.setLinks( -1, -1, channel.getInt() );
            record.setInUse( false );
        }
        if ( bitFlag( flags, Record.CREATED_IN_TX ) )
        {
            record.setCreated();
        }

        return record;
    }

    private DynamicRecord readDynamicRecord( ReadableChannel channel ) throws IOException
    {
        // id+type+in_use(byte)+nr_of_bytes(int)+next_block(long)
        long id = channel.getLong();
        assert id >= 0 && id <= (1L << 36) - 1 : id + " is not a valid dynamic record id";
        int type = channel.getInt();
        byte inUseFlag = channel.get();
        boolean inUse = (inUseFlag & Record.IN_USE.byteValue()) != 0;
        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( inUse, type );
        if ( inUse )
        {
            // Some constants have changed in 4.0 to better reflect how they are used. It's not a mistake that the CREATED_IN_TX bit
            // is used in this legacy format for the 'start record' flag. Previously it was using a specific FIRST_IN_CHAIN flag
            // that had the same bit position as CREATED_IN_TX
            record.setStartRecord( (inUseFlag & Record.CREATED_IN_TX) != 0 );
            int nrOfBytes = channel.getInt();
            assert nrOfBytes >= 0 && nrOfBytes < ((1 << 24) - 1) : nrOfBytes
                    + " is not valid for a number of bytes field of " + "a dynamic record";
            long nextBlock = channel.getLong();
            assert (nextBlock >= 0 && nextBlock <= (1L << 36 - 1))
                    || (nextBlock == Record.NO_NEXT_BLOCK.intValue()) : nextBlock
                    + " is not valid for a next record field of " + "a dynamic record";
            record.setNextBlock( nextBlock );
            byte[] data = new byte[nrOfBytes];
            channel.get( data, nrOfBytes );
            record.setData( data );
        }
        return record;
    }

    private <T> int readDynamicRecords( ReadableChannel channel, T target, DynamicRecordAdder<T> adder )
            throws IOException
    {
        int numberOfRecords = channel.getInt();
        assert numberOfRecords >= 0;
        while ( numberOfRecords > 0 )
        {
            DynamicRecord read = readDynamicRecord( channel );
            if ( read == null )
            {
                return -1;
            }
            adder.add( target, read );
            numberOfRecords--;
        }
        return numberOfRecords;
    }

    private PropertyRecord readPropertyRecord( long id, ReadableChannel channel ) throws IOException
    {
        // in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
        // prev_prop_id(long)+next_prop_id(long)
        PropertyRecord record = new PropertyRecord( id );
        byte flags = channel.get(); // 1

        boolean inUse = bitFlag( flags, Record.IN_USE.byteValue() );
        boolean nodeProperty = !bitFlag( flags, Record.REL_PROPERTY.byteValue() );
        boolean requireSecondaryUnit = bitFlag( flags, Record.REQUIRE_SECONDARY_UNIT );
        boolean hasSecondaryUnit = bitFlag( flags, Record.HAS_SECONDARY_UNIT );
        boolean usesFixedReferenceFormat = bitFlag( flags, Record.USES_FIXED_REFERENCE_FORMAT );

        record.setRequiresSecondaryUnit( requireSecondaryUnit );
        record.setUseFixedReferences( usesFixedReferenceFormat );

        long nextProp = channel.getLong(); // 8
        long prevProp = channel.getLong(); // 8
        record.setNextProp( nextProp );
        record.setPrevProp( prevProp );

        long primitiveId = channel.getLong(); // 8
        if ( primitiveId != -1 && nodeProperty )
        {
            record.setNodeId( primitiveId );
        }
        else if ( primitiveId != -1 )
        {
            record.setRelId( primitiveId );
        }
        if ( hasSecondaryUnit )
        {
            record.setSecondaryUnitIdOnLoad( channel.getLong() );
        }
        int nrPropBlocks = channel.get();
        assert nrPropBlocks >= 0;
        if ( nrPropBlocks > 0 )
        {
            record.setInUse( true );
        }
        while ( nrPropBlocks-- > 0 )
        {
            PropertyBlock block = readPropertyBlock( channel );
            if ( block == null )
            {
                return null;
            }
            record.addPropertyBlock( block );
        }
        int deletedRecords = readDynamicRecords( channel, record, PROPERTY_DELETED_DYNAMIC_RECORD_ADDER );
        if ( deletedRecords == -1 )
        {
            return null;
        }
        assert deletedRecords >= 0;
        while ( deletedRecords-- > 0 )
        {
            DynamicRecord read = readDynamicRecord( channel );
            if ( read == null )
            {
                return null;
            }
            record.addDeletedRecord( read );
        }
        if ( (inUse && !record.inUse()) || (!inUse && record.inUse()) )
        {
            throw new IllegalStateException( "Weird, inUse was read in as " + inUse + " but the record is " + record );
        }
        return record;
    }

    private PropertyBlock readPropertyBlock( ReadableChannel channel ) throws IOException
    {
        PropertyBlock toReturn = new PropertyBlock();
        byte blockSize = channel.get(); // the size is stored in bytes // 1
        assert blockSize > 0 && blockSize % 8 == 0 : blockSize + " is not a valid block size value";
        // Read in blocks
        long[] blocks = readLongs( channel, blockSize / 8 );
        assert blocks.length == blockSize / 8 : blocks.length
                + " longs were read in while i asked for what corresponds to " + blockSize;

        assert PropertyType.getPropertyTypeOrThrow( blocks[0] ).calculateNumberOfBlocksUsed(
                blocks[0] ) == blocks.length : blocks.length + " is not a valid number of blocks for type "
                + PropertyType.getPropertyTypeOrThrow( blocks[0] );
        /*
         *  Ok, now we may be ready to return, if there are no DynamicRecords. So
         *  we start building the Object
         */
        toReturn.setValueBlocks( blocks );
        /*
         * Read in existence of DynamicRecords. Remember, this has already been
         * read in the buffer with the blocks, above.
         */
        if ( readDynamicRecords( channel, toReturn, PROPERTY_BLOCK_DYNAMIC_RECORD_ADDER ) == -1 )
        {
            return null;
        }
        return toReturn;
    }

    private long[] readLongs( ReadableChannel channel, int count ) throws IOException
    {
        long[] result = new long[count];
        for ( int i = 0; i < count; i++ )
        {
            result[i] = channel.getLong();
        }
        return result;
    }

    private SchemaRule readSchemaRule( Collection<DynamicRecord> recordsBefore )
    {
        // TODO: Why was this assertion here?
        //            assert first(recordsBefore).inUse() : "Asked to deserialize schema records that were not in
        // use.";
        SchemaRule rule;
        ByteBuffer deserialized = AbstractDynamicStore.concatData( recordsBefore, new byte[100] );
        try
        {
            rule = SchemaRuleSerialization35.deserialize( Iterables.first( recordsBefore ).getId(), deserialized );
        }
        catch ( MalformedSchemaRuleException e )
        {
            return null;
        }
        return rule;
    }

    @Override
    protected Command readIndexAddNodeCommand( ReadableChannel channel ) throws IOException
    {
        IndexCommandHeader header = readIndexCommandHeader( channel );
        Number entityId = header.entityIdNeedsLong ? channel.getLong() : channel.getInt();
        Object value = readIndexValue( header.valueType, channel );
        AddNodeCommand command = new AddNodeCommand();
        command.init( header.indexNameId, entityId.longValue(), header.keyId, value );
        return command;
    }

    @Override
    protected Command readIndexAddRelationshipCommand( ReadableChannel channel ) throws IOException
    {
        IndexCommandHeader header = readIndexCommandHeader( channel );
        Number entityId = header.entityIdNeedsLong ? channel.getLong() : channel.getInt();
        Object value = readIndexValue( header.valueType, channel );
        Number startNode = header.startNodeNeedsLong ? channel.getLong() : channel.getInt();
        Number endNode = header.endNodeNeedsLong ? channel.getLong() : channel.getInt();
        AddRelationshipCommand command = new AddRelationshipCommand();
        command.init( header.indexNameId, entityId.longValue(), header.keyId, value, startNode.longValue(),
                endNode.longValue() );
        return command;
    }

    @Override
    protected Command readIndexRemoveCommand( ReadableChannel channel ) throws IOException
    {
        IndexCommandHeader header = readIndexCommandHeader( channel );
        Number entityId = header.entityIdNeedsLong ? channel.getLong() : channel.getInt();
        Object value = readIndexValue( header.valueType, channel );
        RemoveCommand command = new RemoveCommand();
        command.init( header.indexNameId, header.entityType, entityId.longValue(), header.keyId, value );
        return command;
    }

    @Override
    protected Command readIndexDeleteCommand( ReadableChannel channel ) throws IOException
    {
        IndexCommandHeader header = readIndexCommandHeader( channel );
        IndexCommand.DeleteCommand command = new IndexCommand.DeleteCommand();
        command.init( header.indexNameId, header.entityType );
        return command;
    }

    @Override
    protected Command readIndexCreateCommand( ReadableChannel channel ) throws IOException
    {
        IndexCommandHeader header = readIndexCommandHeader( channel );
        Map<String,String> config = read2bMap( channel );
        CreateCommand command = new CreateCommand();
        command.init( header.indexNameId, header.entityType, config );
        return command;
    }

    @Override
    protected Command readIndexDefineCommand( ReadableChannel channel ) throws IOException
    {
        readIndexCommandHeader( channel );
        MutableObjectIntMap<String> indexNames = readMap( channel );
        MutableObjectIntMap<String> keys = readMap( channel );
        IndexDefineCommand command = new IndexDefineCommand();
        command.init( indexNames, keys );
        return command;
    }

    @Override
    protected Command readNodeCountsCommand( ReadableChannel channel ) throws IOException
    {
        int labelId = channel.getInt();
        long delta = channel.getLong();
        return new Command.NodeCountsCommand( this, labelId, delta );
    }

    @Override
    protected Command readRelationshipCountsCommand( ReadableChannel channel ) throws IOException
    {
        int startLabelId = channel.getInt();
        int typeId = channel.getInt();
        int endLabelId = channel.getInt();
        long delta = channel.getLong();
        return new Command.RelationshipCountsCommand( this, startLabelId, typeId, endLabelId, delta );
    }

    @VisibleForTesting // or rather _implemented_ for testing
    @Override
    public void writeNeoStoreCommand( WritableChannel channel, Command.NeoStoreCommand command ) throws IOException
    {
        channel.put( NeoCommandType.NEOSTORE_COMMAND );
        channel.putLong( command.before.getNextProp() );
        channel.putLong( command.after.getNextProp() );
    }

    private MutableObjectIntMap<String> readMap( ReadableChannel channel ) throws IOException
    {
        int size = getUnsignedShort( channel );
        MutableObjectIntMap<String> result = new ObjectIntHashMap<>( size );
        for ( int i = 0; i < size; i++ )
        {
            String key = read2bLengthAndString( channel );
            int id = getUnsignedShort( channel );
            if ( key == null )
            {
                return null;
            }
            result.put( key, id );
        }
        return result;
    }

    private int getUnsignedShort( ReadableChannel channel ) throws IOException
    {
        int result = channel.getShort() & 0xFFFF;
        return result == 0xFFFF ? -1 : result;
    }

    private IndexCommandHeader readIndexCommandHeader( ReadableChannel channel ) throws IOException
    {
        byte firstHeaderByte = channel.get();
        byte valueType = (byte) ((firstHeaderByte & 0x1C) >> 2);
        byte entityType = (byte) ((firstHeaderByte & 0x2) >> 1);
        boolean entityIdNeedsLong = (firstHeaderByte & 0x1) > 0;
        byte secondHeaderByte = channel.get();
        boolean startNodeNeedsLong = (secondHeaderByte & 0x80) > 0;
        boolean endNodeNeedsLong = (secondHeaderByte & 0x40) > 0;
        int indexNameId = getUnsignedShort( channel );
        int keyId = getUnsignedShort( channel );
        return new IndexCommandHeader( valueType, entityType, entityIdNeedsLong, indexNameId, startNodeNeedsLong,
                endNodeNeedsLong, keyId );
    }

    private Object readIndexValue( byte valueType, ReadableChannel channel ) throws IOException
    {
        switch ( valueType )
        {
        case IndexCommand.VALUE_TYPE_NULL:
            return null;
        case IndexCommand.VALUE_TYPE_SHORT:
            return channel.getShort();
        case IndexCommand.VALUE_TYPE_INT:
            return channel.getInt();
        case IndexCommand.VALUE_TYPE_LONG:
            return channel.getLong();
        case IndexCommand.VALUE_TYPE_FLOAT:
            return channel.getFloat();
        case IndexCommand.VALUE_TYPE_DOUBLE:
            return channel.getDouble();
        case IndexCommand.VALUE_TYPE_STRING:
            return read3bLengthAndString( channel );
        default:
            throw new RuntimeException( "Unknown value type " + valueType );
        }
    }

    private static final class IndexCommandHeader
    {
        byte valueType;
        byte entityType;
        boolean entityIdNeedsLong;
        int indexNameId;
        boolean startNodeNeedsLong;
        boolean endNodeNeedsLong;
        int keyId;

        IndexCommandHeader( byte valueType, byte entityType, boolean entityIdNeedsLong, int indexNameId,
                boolean startNodeNeedsLong, boolean endNodeNeedsLong, int keyId )
        {
            this.valueType = valueType;
            this.entityType = entityType;
            this.entityIdNeedsLong = entityIdNeedsLong;
            this.indexNameId = indexNameId;
            this.startNodeNeedsLong = startNodeNeedsLong;
            this.endNodeNeedsLong = endNodeNeedsLong;
            this.keyId = keyId;
        }
    }
}
