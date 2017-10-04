/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatSettings;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.LogProvider;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.store.DynamicArrayStore.getRightArray;
import static org.neo4j.kernel.impl.store.NoStoreHeaderFormat.NO_STORE_HEADER_FORMAT;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

/**
 * Implementation of the property store. This implementation has two dynamic
 * stores. One used to store keys and another for string property values.
 */
public class PropertyStore extends CommonAbstractStore<PropertyRecord,NoStoreHeader>
{
    public abstract static class Configuration extends CommonAbstractStore.Configuration
    {
    }

    public static final String TYPE_DESCRIPTOR = "PropertyStore";

    private final DynamicStringStore stringStore;
    private final PropertyKeyTokenStore propertyKeyTokenStore;
    private final DynamicArrayStore arrayStore;

    public PropertyStore(
            File fileName,
            Config configuration,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            DynamicStringStore stringPropertyStore,
            PropertyKeyTokenStore propertyKeyTokenStore,
            DynamicArrayStore arrayPropertyStore,
            RecordFormats recordFormats,
            OpenOption... openOptions )
    {
        super( fileName, configuration, IdType.PROPERTY, idGeneratorFactory, pageCache, logProvider, TYPE_DESCRIPTOR,
                recordFormats.property(), NO_STORE_HEADER_FORMAT, recordFormats.storeVersion(), openOptions );
        this.stringStore = stringPropertyStore;
        this.propertyKeyTokenStore = propertyKeyTokenStore;
        this.arrayStore = arrayPropertyStore;
    }

    @Override
    public <FAILURE extends Exception> void accept( RecordStore.Processor<FAILURE> processor, PropertyRecord record )
            throws FAILURE
    {
        processor.processProperty( this, record );
    }

    public DynamicStringStore getStringStore()
    {
        return stringStore;
    }

    public DynamicArrayStore getArrayStore()
    {
        return arrayStore;
    }

    public PropertyKeyTokenStore getPropertyKeyTokenStore()
    {
        return propertyKeyTokenStore;
    }

    @Override
    public void updateRecord( PropertyRecord record )
    {
        updatePropertyBlocks( record );
        super.updateRecord( record );
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
            PropertyType recordType = valueRecord.getType();
            if ( recordType == PropertyType.STRING )
            {
                stringStore.updateRecord( valueRecord );
            }
            else if ( recordType == PropertyType.ARRAY )
            {
                arrayStore.updateRecord( valueRecord );
            }
            else
            {
                throw new InvalidRecordException( "Unknown dynamic record"
                        + valueRecord );
            }
        }
    }

    @Override
    public void ensureHeavy( PropertyRecord record )
    {
        for ( PropertyBlock block : record )
        {
            ensureHeavy( block );
        }
    }

    public void ensureHeavy( PropertyBlock block )
    {
        if ( !block.isLight() )
        {
            return;
        }

        PropertyType type = block.getType();
        RecordStore<DynamicRecord> dynamicStore = dynamicStoreForValueType( type );
        if ( dynamicStore == null )
        {
            return;
        }

        try ( Cursor<DynamicRecord> dynamicRecords = dynamicStore.newRecordCursor( dynamicStore.newRecord() )
                .acquire( block.getSingleValueLong(), NORMAL ) )
        {
            while ( dynamicRecords.next() )
            {
                dynamicRecords.get().setType( type.intValue() );
                block.addValueRecord( dynamicRecords.get().clone() );
            }
        }
    }

    private RecordStore<DynamicRecord> dynamicStoreForValueType( PropertyType type )
    {
        switch ( type )
        {
        case ARRAY: return arrayStore;
        case STRING: return stringStore;
        default: return null;
        }
    }

    public Value getValue( PropertyBlock propertyBlock )
    {
        return propertyBlock.getType().value( propertyBlock, this );
    }

    public static void allocateStringRecords( Collection<DynamicRecord> target, byte[] chars,
            DynamicRecordAllocator allocator )
    {
        AbstractDynamicStore.allocateRecordsFromBytes( target, chars, allocator );
    }

    public static void allocateArrayRecords( Collection<DynamicRecord> target, Object array,
            DynamicRecordAllocator allocator )
    {
        DynamicArrayStore.allocateRecords( target, array, allocator );
    }

    public void encodeValue( PropertyBlock block, int keyId, Value value )
    {
        encodeValue( block, keyId, value, stringStore, arrayStore );
    }

    public static void encodeValue( PropertyBlock block, int keyId, Value value,
            DynamicRecordAllocator stringAllocator, DynamicRecordAllocator arrayAllocator )
    {
        // TODO: use ValueWriter
        Object asObject = value.asObject();
        if ( asObject instanceof String )
        {   // Try short string first, i.e. inlined in the property block
            String string = (String) asObject;
            if ( LongerShortString.encode( keyId, string, block, PropertyType.getPayloadSize() ) )
            {
                return;
            }

            // Fall back to dynamic string store
            byte[] encodedString = encodeString( string );
            List<DynamicRecord> valueRecords = new ArrayList<>();
            allocateStringRecords( valueRecords, encodedString, stringAllocator );
            setSingleBlockValue( block, keyId, PropertyType.STRING, Iterables.first( valueRecords ).getId() );
            for ( DynamicRecord valueRecord : valueRecords )
            {
                valueRecord.setType( PropertyType.STRING.intValue() );
            }
            block.setValueRecords( valueRecords );
        }
        else if ( asObject instanceof Integer )
        {
            setSingleBlockValue( block, keyId, PropertyType.INT, ((Integer) asObject).longValue() );
        }
        else if ( asObject instanceof Boolean )
        {
            setSingleBlockValue( block, keyId, PropertyType.BOOL, (Boolean) asObject ? 1L : 0L );
        }
        else if ( asObject instanceof Float )
        {
            setSingleBlockValue( block, keyId, PropertyType.FLOAT, Float.floatToRawIntBits( (Float) asObject ) );
        }
        else if ( asObject instanceof Long )
        {

            long keyAndType = keyId | (((long) PropertyType.LONG.intValue()) <<
                                       StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS);
            if ( ShortArray.LONG.getRequiredBits( (Long) asObject ) <= 35 )
            {   // We only need one block for this value, special layout compared to, say, an integer
                block.setSingleBlock( keyAndType | (1L << 28) | ((Long) asObject << 29) );
            }
            else
            {   // We need two blocks for this value
                block.setValueBlocks( new long[]{keyAndType, (Long) asObject} );
            }
        }
        else if ( asObject instanceof Double )
        {
            block.setValueBlocks( new long[]{ keyId |
                    (((long) PropertyType.DOUBLE.intValue()) << StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS),
                    Double.doubleToRawLongBits( (Double) asObject )} );
        }
        else if ( asObject instanceof Byte )
        {
            setSingleBlockValue( block, keyId, PropertyType.BYTE, ((Byte) asObject).longValue() );
        }
        else if ( asObject instanceof Character )
        {
            setSingleBlockValue( block, keyId, PropertyType.CHAR, (Character) asObject );
        }
        else if ( asObject instanceof Short )
        {
            setSingleBlockValue( block, keyId, PropertyType.SHORT, ((Short) asObject).longValue() );
        }
        else if ( asObject.getClass().isArray() )
        {   // Try short array first, i.e. inlined in the property block
            if ( ShortArray.encode( keyId, asObject, block, PropertyType.getPayloadSize() ) )
            {
                return;
            }

            // Fall back to dynamic array store
            List<DynamicRecord> arrayRecords = new ArrayList<>();
            allocateArrayRecords( arrayRecords, asObject, arrayAllocator );
            setSingleBlockValue( block, keyId, PropertyType.ARRAY, Iterables.first( arrayRecords ).getId() );
            for ( DynamicRecord valueRecord : arrayRecords )
            {
                valueRecord.setType( PropertyType.ARRAY.intValue() );
            }
            block.setValueRecords( arrayRecords );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown property type on: " + asObject + ", " + asObject.getClass() );
        }
    }

    public static void setSingleBlockValue( PropertyBlock block, int keyId, PropertyType type, long longValue )
    {
        block.setSingleBlock( singleBlockLongValue( keyId, type, longValue ) );
    }

    public static long singleBlockLongValue( int keyId, PropertyType type, long longValue )
    {
        return keyId | (((long) type.intValue()) << StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS) |
               (longValue << 28);
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
        Pair<byte[], byte[]> source = stringStore.readFullByteArray( dynamicRecords, PropertyType.STRING );
        // A string doesn't have a header in the data array
        return decodeString( source.other() );
    }

    public Value getArrayFor( PropertyBlock propertyBlock )
    {
        ensureHeavy( propertyBlock );
        return getArrayFor( propertyBlock.getValueRecords() );
    }

    public Value getArrayFor( Iterable<DynamicRecord> records )
    {
        return getRightArray( arrayStore.readFullByteArray( records, PropertyType.ARRAY ) );
    }

    @Override
    public String toString()
    {
        return super.toString() + "[blocksPerRecord:" + PropertyType.getPayloadSizeLongs() + "]";
    }

    public Collection<PropertyRecord> getPropertyRecordChain( long firstRecordId )
    {
        long nextProp = firstRecordId;
        List<PropertyRecord> toReturn = new LinkedList<>();
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = new PropertyRecord( nextProp );
            getRecord( nextProp, propRecord, RecordLoad.NORMAL );
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
                getRecord( nextProp, propRecord = newRecord(), RecordLoad.NORMAL );
            }
            toReturn.add( propRecord );
            nextProp = propRecord.getNextProp();
        }
        return toReturn;
    }

    @Override
    public PropertyRecord newRecord()
    {
        return new PropertyRecord( -1 );
    }
}
