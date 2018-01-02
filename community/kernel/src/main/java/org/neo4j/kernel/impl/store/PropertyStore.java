/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.cursor.GenericCursor;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.PropertyPhysicalToLogicalConverter;
import org.neo4j.kernel.impl.api.store.PropertyRecordCursor;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.state.PropertyRecordChange;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.kernel.impl.store.DynamicArrayStore.getRightArray;

/**
 * Implementation of the property store. This implementation has two dynamic
 * stores. One used to store keys and another for string property values.
 */
public class PropertyStore extends AbstractRecordStore<PropertyRecord>
{
    public static abstract class Configuration extends AbstractStore.Configuration
    {
    }

    public static final int DEFAULT_DATA_BLOCK_SIZE = 120;
    public static final int DEFAULT_PAYLOAD_SIZE = 32;

    public static final String TYPE_DESCRIPTOR = "PropertyStore";

    public static final int RECORD_SIZE = 1/*next and prev high bits*/
            + 4/*next*/
            + 4/*prev*/
            + DEFAULT_PAYLOAD_SIZE /*property blocks*/;
    // = 41

    private final DynamicStringStore stringPropertyStore;
    private final PropertyKeyTokenStore propertyKeyTokenStore;
    private final DynamicArrayStore arrayPropertyStore;
    private final PropertyPhysicalToLogicalConverter physicalToLogicalConverter;

    public PropertyStore(
            File fileName,
            Config configuration,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            DynamicStringStore stringPropertyStore,
            PropertyKeyTokenStore propertyKeyTokenStore,
            DynamicArrayStore arrayPropertyStore )
    {
        super( fileName, configuration, IdType.PROPERTY, idGeneratorFactory, pageCache, logProvider );
        this.stringPropertyStore = stringPropertyStore;
        this.propertyKeyTokenStore = propertyKeyTokenStore;
        this.arrayPropertyStore = arrayPropertyStore;
        this.physicalToLogicalConverter = new PropertyPhysicalToLogicalConverter( this );
    }

    @Override
    public <FAILURE extends Exception> void accept( RecordStore.Processor<FAILURE> processor, PropertyRecord record )
            throws FAILURE
    {
        processor.processProperty( this, record );
    }

    public DynamicStringStore getStringStore()
    {
        return stringPropertyStore;
    }

    public DynamicArrayStore getArrayStore()
    {
        return arrayPropertyStore;
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
    }

    @Override
    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    @Override
    public int getRecordHeaderSize()
    {
        return RECORD_SIZE - DEFAULT_PAYLOAD_SIZE;
    }

    public PropertyKeyTokenStore getPropertyKeyTokenStore()
    {
        return propertyKeyTokenStore;
    }

    @Override
    public void updateRecord( PropertyRecord record )
    {
        long pageId = pageIdForRecord( record.getId() );
        updatePropertyBlocks( record );
        try ( PageCursor cursor = storeFile.io( pageId, PF_EXCLUSIVE_LOCK ) )
        {
            if ( cursor.next() ) // should always be true
            {
                do
                {
                    updateRecord( record, cursor );
                } while ( cursor.shouldRetry() );
            }
            else
            {
                throw new UnderlyingStorageException(
                        "Could not pin page[" + pageId +
                                " exclusively for updateRecord: " + record );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }

    }

    @Override
    public void forceUpdateRecord( PropertyRecord record )
    {
        updateRecord( record ); // TODO: should we do something special for property records?
    }

    private void updateRecord( PropertyRecord record, PageCursor cursor )
    {
        long id = record.getId();
        cursor.setOffset( (int) (id * RECORD_SIZE % storeFile.pageSize()) );
        if ( record.inUse() )
        {
            // Set up the record header
            short prevModifier = record.getPrevProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0
                    : (short) ((record.getPrevProp() & 0xF00000000L) >> 28);
            short nextModifier = record.getNextProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0
                    : (short) ((record.getNextProp() & 0xF00000000L) >> 32);
            byte modifiers = (byte) (prevModifier | nextModifier);
            /*
             * [pppp,nnnn] previous, next high bits
             */
            cursor.putByte( modifiers );
            cursor.putInt( (int) record.getPrevProp() );
            cursor.putInt( (int) record.getNextProp() );

            // Then go through the blocks
            int longsAppended = 0; // For marking the end of blocks
            for ( PropertyBlock block : record )
            {
                long[] propBlockValues = block.getValueBlocks();
                for ( long propBlockValue : propBlockValues )
                {
                    cursor.putLong( propBlockValue );
                }

                longsAppended += propBlockValues.length;
            }
            if ( longsAppended < PropertyType.getPayloadSizeLongs() )
            {
                cursor.putLong( 0 );
            }
        }
        else
        {
            freeId( id );
            // skip over the record header, nothing useful there
            cursor.setOffset( cursor.getOffset() + 9 );
            cursor.putLong( 0 );
        }
    }

    private void updatePropertyBlocks( PropertyRecord record )
    {
        if ( record.inUse() )
        {
            // Go through the blocks
            for ( PropertyBlock block : record )
            {
                /*
                 * For each block we need to update its dynamic record chain if
                 * it is just created. Deleted dynamic records are in the property
                 * record and dynamic records are never modified. Also, they are
                 * assigned as a whole, so just checking the first should be enough.
                 */
                if ( !block.isLight()
                        && block.getValueRecords().get( 0 ).isCreated() )
                {
                    updateDynamicRecords( block.getValueRecords() );
                }
            }
        }
        updateDynamicRecords( record.getDeletedRecords() );
    }

    private void updateDynamicRecords( List<DynamicRecord> records )
    {
        for ( DynamicRecord valueRecord : records )
        {
            if ( valueRecord.getType() == PropertyType.STRING.intValue() )
            {
                stringPropertyStore.updateRecord( valueRecord );
            }
            else if ( valueRecord.getType() == PropertyType.ARRAY.intValue() )
            {
                arrayPropertyStore.updateRecord( valueRecord );
            }
            else
            {
                throw new InvalidRecordException( "Unknown dynamic record"
                        + valueRecord );
            }
        }
    }

    public PropertyRecord getLightRecord( long id )
    {
        return getRecord( id );
    }

    public void ensureHeavy( PropertyBlock block )
    {
        if ( block.getType() == PropertyType.STRING )
        {
            loadPropertyBlock( block, stringPropertyStore );
        }
        else if ( block.getType() == PropertyType.ARRAY )
        {
            loadPropertyBlock( block, arrayPropertyStore );
        }
    }

    private static void loadPropertyBlock( PropertyBlock block, AbstractDynamicStore dynamicStore )
    {
        if ( block.isLight() )
        {
            long startBlockId = block.getSingleValueLong();
            try ( GenericCursor<DynamicRecord> cursor = dynamicStore.getRecordsCursor( startBlockId ) )
            {
                while ( cursor.next() )
                {
                    cursor.get().setType( block.getType().intValue() );
                    block.addValueRecord( cursor.get().clone() );
                }
            }
        }
        else
        {
            for ( DynamicRecord dynamicRecord : block.getValueRecords() )
            {
                dynamicStore.ensureHeavy( dynamicRecord );
            }
        }
    }

    @Override
    public PropertyRecord getRecord( long id )
    {
        return getRecord( new PropertyRecord( id ) );
    }

    public PropertyRecord getRecord( PropertyRecord record )
    {
        try ( PageCursor cursor = storeFile.io( pageIdForRecord( record.getId() ), PF_SHARED_LOCK ) )
        {
            PropertyRecord tmpRecord = null;
            if ( cursor.next() )
            {
                do
                {
                    tmpRecord = getRecord( cursor, record );
                } while ( cursor.shouldRetry() );
            }

            if ( tmpRecord == null || !tmpRecord.inUse() )
            {
                throw new InvalidRecordException( "PropertyRecord[" + record.getId() + "] not in use" );
            }

            return record;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    public PageCursor newReadCursor( long recordId ) throws IOException
    {
        PageCursor cursor = storeFile.io( pageIdForRecord( recordId ), PF_SHARED_LOCK );
        try
        {
            if ( !cursor.next() )
            {
                throw new IOException( "Record not found: " + recordId );
            }
            cursor.setOffset( (int) (recordId * RECORD_SIZE % storeFile.pageSize()) );
            return cursor;
        }
        catch ( Throwable failure )
        {
            cursor.close();
            throw failure;
        }
    }

    public PropertyRecord getRecord( PropertyRecord record, PageCursor cursor )
    {
        try
        {
            PropertyRecord tmpRecord = null;
            if ( cursor.next( pageIdForRecord( record.getId() ) ) )
            {
                do
                {
                    tmpRecord = getRecord( cursor, record );
                } while ( cursor.shouldRetry() );
            }

            if ( tmpRecord == null || !tmpRecord.inUse() )
            {
                throw new InvalidRecordException( "PropertyRecord[" + record.getId() + "] not in use" );
            }
            record.verifyRecordIsWellFormed();

            return record;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public PropertyRecord forceGetRecord( long id )
    {
        try ( PageCursor cursor = storeFile.io( pageIdForRecord( id ), PF_SHARED_LOCK ) )
        {
            PropertyRecord record = new PropertyRecord( id );
            if ( cursor.next() )
            {
                do
                {
                    record = getRecord( cursor, record );
                } while ( cursor.shouldRetry() );
            }
            return record == null ? new PropertyRecord( id ) : record;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private PropertyRecord getRecordFromBuffer( long id, PageCursor cursor )
    {
        return getRecordFromBuffer( cursor, new PropertyRecord( id ) );
    }

    private PropertyRecord getRecordFromBuffer( PageCursor cursor, PropertyRecord record )
    {
        record.clearPropertyBlocks();

        int offsetAtBeginning = cursor.getOffset();

        /*
         * [pppp,nnnn] previous, next high bits
         */
        byte modifiers = cursor.getByte();
        long prevMod = (modifiers & 0xF0L) << 28;
        long nextMod = (modifiers & 0x0FL) << 32;
        long prevProp = cursor.getUnsignedInt();
        long nextProp = cursor.getUnsignedInt();
        record.setPrevProp( longFromIntAndMod( prevProp, prevMod ) );
        record.setNextProp( longFromIntAndMod( nextProp, nextMod ) );

        while ( cursor.getOffset() - offsetAtBeginning < RECORD_SIZE )
        {
            PropertyBlock newBlock = getPropertyBlock( cursor, record );
            if ( newBlock != null )
            {
                record.addPropertyBlock( newBlock );
                record.setInUse( true );
            }
            else
            {
                // We assume that storage is defragged
                break;
            }
        }
        return record;
    }

    private PropertyRecord getRecord( PageCursor cursor, PropertyRecord record )
    {
        cursor.setOffset( (int) (record.getId() * RECORD_SIZE % storeFile.pageSize()) );
        return getRecordFromBuffer( cursor, record );
    }

    /*
     * It is assumed that the argument does hold a property block - all zeros is
     * a valid (not in use) block, so even if the Bits object has been exhausted a
     * result is returned, that has inUse() return false. Also, the argument is not
     * touched.
     */
    private PropertyBlock getPropertyBlock( PageCursor cursor, PropertyRecord record )
    {
        long header = cursor.getLong();
        PropertyType type = PropertyType.getPropertyType( header, true );
        if ( type == null )
        {
            return null;
        }
        PropertyBlock toReturn = new PropertyBlock();
        // toReturn.setInUse( true );
        int numBlocks = type.calculateNumberOfBlocksUsed( header );
        if ( numBlocks == PropertyType.BLOCKS_USED_FOR_BAD_TYPE_OR_ENCODING )
        {
            record.setMalformedMessage( "Invalid type or encoding of property block" );
            return null;
        }
        if ( numBlocks > PropertyType.getPayloadSizeLongs() )
        {
            // We most likely got an inconsistent read, so return null to signal failure for now
            record.setMalformedMessage(
                    "I was given an array of size " + numBlocks +", but I wanted it to be " +
                    PropertyType.getPayloadSizeLongs() );
            return null;
        }
        long[] blockData = new long[numBlocks];
        blockData[0] = header; // we already have that
        for ( int i = 1; i < numBlocks; i++ )
        {
            blockData[i] = cursor.getLong();
        }
        toReturn.setValueBlocks( blockData );
        return toReturn;
    }

    public Object getValue( PropertyBlock propertyBlock )
    {
        return propertyBlock.getType().getValue( propertyBlock, this );
    }

    public static void allocateStringRecords( Collection<DynamicRecord> target, byte[] chars,
            DynamicRecordAllocator allocator )
    {
        AbstractDynamicStore.allocateRecordsFromBytes( target, chars,
                IteratorUtil.<DynamicRecord>emptyIterator(), allocator );
    }

    public static void allocateArrayRecords( Collection<DynamicRecord> target, Object array,
            DynamicRecordAllocator allocator )
    {
        DynamicArrayStore.allocateRecords( target, array, IteratorUtil.<DynamicRecord>emptyIterator(), allocator );
    }

    public void encodeValue( PropertyBlock block, int keyId, Object value )
    {
        encodeValue( block, keyId, value, stringPropertyStore, arrayPropertyStore );
    }

    public static void encodeValue( PropertyBlock block, int keyId, Object value,
            DynamicRecordAllocator stringAllocator, DynamicRecordAllocator arrayAllocator )
    {
        if ( value instanceof String )
        {   // Try short string first, i.e. inlined in the property block
            String string = (String) value;
            if ( LongerShortString.encode( keyId, string, block, PropertyType.getPayloadSize() ) )
            {
                return;
            }

            // Fall back to dynamic string store
            byte[] encodedString = encodeString( string );
            List<DynamicRecord> valueRecords = new ArrayList<>();
            allocateStringRecords( valueRecords, encodedString, stringAllocator );
            setSingleBlockValue( block, keyId, PropertyType.STRING, first( valueRecords ).getId() );
            for ( DynamicRecord valueRecord : valueRecords )
            {
                valueRecord.setType( PropertyType.STRING.intValue() );
            }
            block.setValueRecords( valueRecords );
        }
        else if ( value instanceof Integer )
        {
            setSingleBlockValue( block, keyId, PropertyType.INT, ((Integer) value).longValue() );
        }
        else if ( value instanceof Boolean )
        {
            setSingleBlockValue( block, keyId, PropertyType.BOOL, ((Boolean) value ? 1L : 0L) );
        }
        else if ( value instanceof Float )
        {
            setSingleBlockValue( block, keyId, PropertyType.FLOAT, Float.floatToRawIntBits( (Float) value ) );
        }
        else if ( value instanceof Long )
        {
            long keyAndType = keyId | (((long) PropertyType.LONG.intValue()) << 24);
            if ( ShortArray.LONG.getRequiredBits( (Long) value ) <= 35 )
            {   // We only need one block for this value, special layout compared to, say, an integer
                block.setSingleBlock( keyAndType | (1L << 28) | ((Long) value << 29) );
            }
            else
            {   // We need two blocks for this value
                block.setValueBlocks( new long[]{keyAndType, (Long) value} );
            }
        }
        else if ( value instanceof Double )
        {
            block.setValueBlocks( new long[]{
                    keyId | (((long) PropertyType.DOUBLE.intValue()) << 24),
                    Double.doubleToRawLongBits( (Double) value )} );
        }
        else if ( value instanceof Byte )
        {
            setSingleBlockValue( block, keyId, PropertyType.BYTE, ((Byte) value).longValue() );
        }
        else if ( value instanceof Character )
        {
            setSingleBlockValue( block, keyId, PropertyType.CHAR, (Character) value );
        }
        else if ( value instanceof Short )
        {
            setSingleBlockValue( block, keyId, PropertyType.SHORT, ((Short) value).longValue() );
        }
        else if ( value.getClass().isArray() )
        {   // Try short array first, i.e. inlined in the property block
            if ( ShortArray.encode( keyId, value, block, PropertyType.getPayloadSize() ) )
            {
                return;
            }

            // Fall back to dynamic array store
            List<DynamicRecord> arrayRecords = new ArrayList<>();
            allocateArrayRecords( arrayRecords, value, arrayAllocator );
            setSingleBlockValue( block, keyId, PropertyType.ARRAY, first( arrayRecords ).getId() );
            for ( DynamicRecord valueRecord : arrayRecords )
            {
                valueRecord.setType( PropertyType.ARRAY.intValue() );
            }
            block.setValueRecords( arrayRecords );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown property type on: " + value + ", " + value.getClass() );
        }
    }

    public static void setSingleBlockValue( PropertyBlock block, int keyId, PropertyType type, long longValue )
    {
        block.setSingleBlock( singleBlockLongValue( keyId, type, longValue ) );
    }

    public static long singleBlockLongValue( int keyId, PropertyType type, long longValue )
    {
        return keyId | (((long) type.intValue()) << 24) | (longValue << 28);
    }

    public static byte[] encodeString( String string )
    {
        return UTF8.encode( string );
    }

    public static String decodeString( byte[] byteArray )
    {
        return UTF8.decode( byteArray );
    }

    public String getStringFor( PropertyBlock propertyBlock )
    {
        ensureHeavy( propertyBlock );
        return getStringFor( propertyBlock.getValueRecords() );
    }

    public String getStringFor( Collection<DynamicRecord> dynamicRecords )
    {
        Pair<byte[], byte[]> source = stringPropertyStore.readFullByteArray( dynamicRecords, PropertyType.STRING );
        // A string doesn't have a header in the data array
        return decodeString( source.other() );
    }

    public Object getArrayFor( PropertyBlock propertyBlock )
    {
        ensureHeavy( propertyBlock );
        return getArrayFor( propertyBlock.getValueRecords() );
    }

    public Object getArrayFor( Iterable<DynamicRecord> records )
    {
        return getRightArray( arrayPropertyStore.readFullByteArray( records, PropertyType.ARRAY ) );
    }

    public int getStringBlockSize()
    {
        return stringPropertyStore.getBlockSize();
    }

    public int getArrayBlockSize()
    {
        return arrayPropertyStore.getBlockSize();
    }

    @Override
    public String toString()
    {
        return super.toString() + "[blocksPerRecord:" + PropertyType.getPayloadSizeLongs() + "]";
    }

    public PropertyRecordCursor getPropertyRecordCursor( PropertyRecordCursor cursor, long firstRecordId )
    {
        if ( cursor == null )
        {
            cursor = new PropertyRecordCursor( new PropertyRecord( -1 ), this );
        }

        cursor.init( firstRecordId );

        return cursor;
    }

    public Collection<PropertyRecord> getPropertyRecordChain( long firstRecordId )
    {
        long nextProp = firstRecordId;
        List<PropertyRecord> toReturn = new LinkedList<>();
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = getLightRecord( nextProp );
            toReturn.add( propRecord );
            nextProp = propRecord.getNextProp();
        }
        return toReturn;
    }

    public Collection<PropertyRecord> getPropertyRecordChain( long firstRecordId,
            PrimitiveLongObjectMap<PropertyRecord> propertyLookup )
    {
        long nextProp = firstRecordId;
        List<PropertyRecord> toReturn = new ArrayList<>();
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = propertyLookup.get( nextProp );
            if ( propRecord == null )
            {
                propRecord = getLightRecord( nextProp );
            }
            toReturn.add( propRecord );
            nextProp = propRecord.getNextProp();
        }
        return toReturn;
    }

    public void toLogicalUpdates( Collection<NodePropertyUpdate> target,
            Iterable<PropertyRecordChange> changes,
            long[] nodeLabelsBefore,
            long[] nodeLabelsAfter )
    {
        physicalToLogicalConverter.apply( target, changes, nodeLabelsBefore, nodeLabelsAfter );
    }

    /**
     * For property records there's no "inUse" byte and we need to read the whole record to
     * see if there are any PropertyBlocks in use in it.
     */
    @Override
    protected boolean isRecordInUse( PageCursor cursor )
    {
        return getRecordFromBuffer( 0 /*id doesn't matter here*/, cursor ).inUse();
    }
}
