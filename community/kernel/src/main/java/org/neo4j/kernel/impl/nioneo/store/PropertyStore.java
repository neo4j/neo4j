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
package org.neo4j.kernel.impl.nioneo.store;

import static org.neo4j.kernel.Config.ARRAY_BLOCK_SIZE;
import static org.neo4j.kernel.Config.STRING_BLOCK_SIZE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Implementation of the property store. This implementation has two dynamic
 * stores. One used to store keys and another for string property values.
 */
public class PropertyStore extends AbstractStore implements Store, RecordStore<PropertyRecord>
{
    public static final int DEFAULT_DATA_BLOCK_SIZE = 120;
    public static final int DEFAULT_PAYLOAD_SIZE = 32;

    public static final String TYPE_DESCRIPTOR = "PropertyStore";

    public static final int RECORD_SIZE = 1/*next and prev high bits*/
    + 4/*next*/
    + 4/*prev*/
    + DEFAULT_PAYLOAD_SIZE /*property blocks*/;
    // = 41

    private DynamicStringStore stringPropertyStore;
    private PropertyIndexStore propertyIndexStore;
    private DynamicArrayStore arrayPropertyStore;

    public PropertyStore( String fileName, Map<?,?> config )
    {
        super( fileName, config, IdType.PROPERTY );
    }

    @Override
    public void accept( RecordStore.Processor processor, PropertyRecord record )
    {
        processor.processProperty( this, record );
    }

    DynamicStringStore getStringStore()
    {
        return stringPropertyStore;
    }

    DynamicArrayStore getArrayStore()
    {
        return arrayPropertyStore;
    }

    @Override
    protected void initStorage()
    {
        stringPropertyStore = new DynamicStringStore( getStorageFileName()
            + ".strings", getConfig(), IdType.STRING_BLOCK );
        propertyIndexStore = new PropertyIndexStore( getStorageFileName()
            + ".index", getConfig() );
        arrayPropertyStore = new DynamicArrayStore( getStorageFileName()
            + ".arrays", getConfig(), IdType.ARRAY_BLOCK );
    }

    @Override
    protected void setRecovered()
    {
        super.setRecovered();
        stringPropertyStore.setRecovered();
        propertyIndexStore.setRecovered();
        arrayPropertyStore.setRecovered();
    }

    @Override
    protected void unsetRecovered()
    {
        super.unsetRecovered();
        stringPropertyStore.unsetRecovered();
        propertyIndexStore.unsetRecovered();
        arrayPropertyStore.unsetRecovered();
    }

    @Override
    protected void closeStorage()
    {
        if ( stringPropertyStore != null )
        {
            stringPropertyStore.close();
            stringPropertyStore = null;
        }
        if ( propertyIndexStore != null )
        {
            propertyIndexStore.close();
            propertyIndexStore = null;
        }
        if ( arrayPropertyStore != null )
        {
            arrayPropertyStore.close();
            arrayPropertyStore = null;
        }
    }

    @Override
    public void flushAll()
    {
        stringPropertyStore.flushAll();
        propertyIndexStore.flushAll();
        arrayPropertyStore.flushAll();
        super.flushAll();
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

    /**
     * Creates a new property store contained in <CODE>fileName</CODE> If
     * filename is <CODE>null</CODE> or the file already exists an
     * <CODE>IOException</CODE> is thrown.
     *
     * @param fileName
     *            File name of the new property store
     * @throws IOException
     *             If unable to create property store or name null
     */
    public static void createStore( String fileName, Map<?,?> config )
    {
        IdGeneratorFactory idGeneratorFactory = (IdGeneratorFactory) config.get(
                IdGeneratorFactory.class );

        createEmptyStore( fileName, buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR ), idGeneratorFactory );
        int stringStoreBlockSize = DEFAULT_DATA_BLOCK_SIZE;
        int arrayStoreBlockSize = DEFAULT_DATA_BLOCK_SIZE;
        try
        {
            String stringBlockSize = (String) config.get( STRING_BLOCK_SIZE );
            String arrayBlockSize = (String) config.get( ARRAY_BLOCK_SIZE );
            if ( stringBlockSize != null )
            {
                int value = Integer.parseInt( stringBlockSize );
                if ( value > 0 )
                {
                    stringStoreBlockSize = value;
                }
            }
            if ( arrayBlockSize != null )
            {
                int value = Integer.parseInt( arrayBlockSize );
                if ( value > 0 )
                {
                    arrayStoreBlockSize = value;
                }
            }
        }
        catch ( Exception e )
        {
            logger.log( Level.WARNING, "Exception creating store", e );
        }

        DynamicStringStore.createStore( fileName + ".strings",
            stringStoreBlockSize, idGeneratorFactory, IdType.STRING_BLOCK );
        PropertyIndexStore.createStore( fileName + ".index", idGeneratorFactory );
        DynamicArrayStore.createStore( fileName + ".arrays",
                arrayStoreBlockSize, idGeneratorFactory );
    }

    private long nextStringBlockId()
    {
        return stringPropertyStore.nextBlockId();
    }

    public void freeStringBlockId( long blockId )
    {
        stringPropertyStore.freeBlockId( blockId );
    }

    private long nextArrayBlockId()
    {
        return arrayPropertyStore.nextBlockId();
    }

    public void freeArrayBlockId( long blockId )
    {
        arrayPropertyStore.freeBlockId( blockId );
    }

    public PropertyIndexStore getIndexStore()
    {
        return propertyIndexStore;
    }

    public void updateRecord( PropertyRecord record, boolean recovered )
    {
        assert recovered;
        setRecovered();
        try
        {
            updateRecord( record );
            registerIdFromUpdateRecord( record.getId() );
        }
        finally
        {
            unsetRecovered();
        }
    }

    public void updateRecord( PropertyRecord record )
    {
        PersistenceWindow window = acquireWindow( record.getId(),
            OperationType.WRITE );
        try
        {
            updateRecord( record, window );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    @Override
    public void forceUpdateRecord( PropertyRecord record )
    {
        updateRecord( record ); // TODO: should we do something special for property records?
    }

    private void updateRecord( PropertyRecord record, PersistenceWindow window )
    {
        long id = record.getId();
        Buffer buffer = window.getOffsettedBuffer( id );
        if ( record.inUse() )
        {
            // Set up the record header
            short prevModifier = record.getPrevProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0
                    : (short) ( ( record.getPrevProp() & 0xF00000000L ) >> 28 );
            short nextModifier = record.getNextProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0
                    : (short) ( ( record.getNextProp() & 0xF00000000L ) >> 32 );
            byte modifiers = (byte) ( prevModifier | nextModifier );
            /*
             * [pppp,nnnn] previous, next high bits
             */
            buffer.put( modifiers );
            buffer.putInt( (int) record.getPrevProp() ).putInt(
                    (int) record.getNextProp() );

            // Then go through the blocks
            int longsAppended = 0; // For marking the end of blocks
            List<PropertyBlock> blocks = record.getPropertyBlocks();
            for ( int i = 0; i < blocks.size(); i++ )
            {
                PropertyBlock block = blocks.get( i );
                long[] propBlockValues = block.getValueBlocks();
                for ( int k = 0; k < propBlockValues.length; k++ )
                {
                    buffer.putLong( propBlockValues[k] );
                }

                longsAppended += propBlockValues.length;
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
            if ( longsAppended < PropertyType.getPayloadSizeLongs() )
            {
                buffer.putLong( 0 );
            }
        }
        else
        {
            if ( !isInRecoveryMode() )
            {
                freeId( id );
            }
            // skip over the record header, nothing useful there
            buffer.setOffset( buffer.getOffset() + 9 );
            buffer.putLong( 0 );
        }
        updateDynamicRecords( record.getDeletedRecords() );
    }

    private void updateDynamicRecords( List<DynamicRecord> records )
    {
        for (int i = 0; i < records.size(); i++)
        {
            DynamicRecord valueRecord = records.get( i );
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
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            PropertyRecord record = getRecord( id, window, RecordLoad.NORMAL );
            return record;
        }
        finally
        {
            releaseWindow( window );
        }
    }

    /*
     * This will add the value records without checking if they are already
     * in the block - so make sure to call this after checking isHeavy() or
     * you will end up with duplicates.
     */
    public void makeHeavy( PropertyBlock record )
    {
        if ( record.getType() == PropertyType.STRING )
        {
            Collection<DynamicRecord> stringRecords = stringPropertyStore.getLightRecords( record.getSingleValueLong() );
            for ( DynamicRecord stringRecord : stringRecords )
            {
                stringRecord.setType( PropertyType.STRING.intValue() );
                record.addValueRecord( stringRecord );
            }
        }
        else if ( record.getType() == PropertyType.ARRAY )
        {
            Collection<DynamicRecord> arrayRecords = arrayPropertyStore.getLightRecords( record.getSingleValueLong() );
            for ( DynamicRecord arrayRecord : arrayRecords )
            {
                arrayRecord.setType( PropertyType.ARRAY.intValue() );
                record.addValueRecord( arrayRecord );
            }
        }
    }

    public PropertyRecord getRecord( long id )
    {
        PropertyRecord record;
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            record = getRecord( id, window, RecordLoad.NORMAL );
        }
        finally
        {
            releaseWindow( window );
        }
        for ( PropertyBlock block : record.getPropertyBlocks() )
        {
            // assert block.inUse();
            if ( block.getType() == PropertyType.STRING )
            {
                Collection<DynamicRecord> stringRecords = stringPropertyStore.getLightRecords( block.getSingleValueLong() );
                for ( DynamicRecord stringRecord : stringRecords )
                {
                    stringRecord.setType( PropertyType.STRING.intValue() );
                    block.addValueRecord( stringRecord );
                }
            }
            else if ( block.getType() == PropertyType.ARRAY )
            {
                Collection<DynamicRecord> arrayRecords = arrayPropertyStore.getLightRecords( block.getSingleValueLong() );
                for ( DynamicRecord arrayRecord : arrayRecords )
                {
                    arrayRecord.setType( PropertyType.ARRAY.intValue() );
                    block.addValueRecord( arrayRecord );
                }
            }
        }
        return record;
    }

    @Override
    public PropertyRecord forceGetRecord( long id )
    {
        PersistenceWindow window = null;
        try
        {
            window = acquireWindow( id, OperationType.READ );
        }
        catch ( InvalidRecordException e )
        {
            return new PropertyRecord( id );
        }
        
        try
        {
            return getRecord( id, window, RecordLoad.FORCE );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    private PropertyRecord getRecordFromBuffer( long id, Buffer buffer )
    {
        int offsetAtBeggining = buffer.getOffset();
        PropertyRecord record = new PropertyRecord( id );

        /*
         * [pppp,nnnn] previous, next high bits
         */
        byte modifiers = buffer.get();
        long prevMod = ( ( modifiers & 0xF0L ) << 28 );
        long nextMod = ( ( modifiers & 0x0FL ) << 32 );
        long prevProp = buffer.getUnsignedInt();
        long nextProp = buffer.getUnsignedInt();
        record.setPrevProp( longFromIntAndMod( prevProp, prevMod ) );
        record.setNextProp( longFromIntAndMod( nextProp, nextMod ) );

        while ( buffer.getOffset() - offsetAtBeggining < RECORD_SIZE )
        {
            PropertyBlock newBlock = getPropertyBlock( buffer );
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

    private PropertyRecord getRecord( long id, PersistenceWindow window, RecordLoad load )
    {
        Buffer buffer = window.getOffsettedBuffer( id );
        PropertyRecord toReturn = getRecordFromBuffer( id, buffer );
        if ( !toReturn.inUse() && load != RecordLoad.FORCE )
        {
            throw new InvalidRecordException( "Record[" + id + "] not in use" );
        }
        return toReturn;
    }

    /*
     * It is assumed that the argument does hold a property block - all zeros is
     * a valid (not in use) block, so even if the Bits object has been exhausted a
     * result is returned, that has inUse() return false. Also, the argument is not
     * touched.
     */
    private static PropertyBlock getPropertyBlock( Buffer buffer )
    {

        long header = buffer.getLong();
        PropertyType type = PropertyType.getPropertyType( header, true );
        if ( type == null )
        {
            return null;
        }
        PropertyBlock toReturn = new PropertyBlock();
        // toReturn.setInUse( true );
        int numBlocks = type.calculateNumberOfBlocksUsed( header );
        long[] blockData = new long[numBlocks];
        blockData[0] = header; // we already have that
        for ( int i = 1; i < numBlocks; i++ )
        {
            blockData[i] = buffer.getLong();
        }
        toReturn.setValueBlocks( blockData );
        return toReturn;
    }

    public Object getValue( PropertyBlock propertyBlock )
    {
        return propertyBlock.getType().getValue( propertyBlock, this );
    }

    @Override
    public void makeStoreOk()
    {
        propertyIndexStore.makeStoreOk();
        stringPropertyStore.makeStoreOk();
        arrayPropertyStore.makeStoreOk();
        super.makeStoreOk();
    }

    @Override
    public void rebuildIdGenerators()
    {
        propertyIndexStore.rebuildIdGenerators();
        stringPropertyStore.rebuildIdGenerators();
        arrayPropertyStore.rebuildIdGenerators();
        super.rebuildIdGenerators();
    }

    public void updateIdGenerators()
    {
        propertyIndexStore.updateIdGenerators();
        stringPropertyStore.updateHighId();
        arrayPropertyStore.updateHighId();
        this.updateHighId();
    }

    private Collection<DynamicRecord> allocateStringRecords( long valueBlockId, byte[] chars )
    {
        return stringPropertyStore.allocateRecords( valueBlockId, chars );
    }

    private Collection<DynamicRecord> allocateArrayRecords( long valueBlockId,
        Object array )
    {
        return arrayPropertyStore.allocateRecords( valueBlockId, array );
    }

    public void encodeValue( PropertyBlock block, int keyId, Object value )
    {
        if ( value instanceof String )
        {   // Try short string first, i.e. inlined in the property block
            String string = (String) value;
            if ( LongerShortString.encode( keyId, string, block,
                    PropertyType.getPayloadSize() ) ) return;

            // Fall back to dynamic string store
            long stringBlockId = nextStringBlockId();
            setSingleBlockValue( block, keyId, PropertyType.STRING, stringBlockId );
            byte[] encodedString = encodeString( string );
            Collection<DynamicRecord> valueRecords = allocateStringRecords( stringBlockId, encodedString );
            for ( DynamicRecord valueRecord : valueRecords )
            {
                valueRecord.setType( PropertyType.STRING.intValue() );
                block.addValueRecord( valueRecord );
            }
        }
        else if ( value instanceof Integer ) setSingleBlockValue( block, keyId, PropertyType.INT, ((Integer)value).longValue() );
        else if ( value instanceof Boolean ) setSingleBlockValue( block, keyId, PropertyType.BOOL, (((Boolean)value).booleanValue()?1L:0L) );
        else if ( value instanceof Float ) setSingleBlockValue( block, keyId, PropertyType.FLOAT, Float.floatToRawIntBits( ((Float) value).floatValue() ) );
        else if ( value instanceof Long )
        {
            long keyAndType = keyId | (((long)PropertyType.LONG.intValue()) << 24);
            if ( ShortArray.LONG.getRequiredBits( value ) <= 35 )
            {   // We only need one block for this value, special layout compared to, say, an integer
                block.setSingleBlock( keyAndType | (1L << 28) |  (((Long)value).longValue() << 29) );
            }
            else
            {   // We need two blocks for this value
                block.setValueBlocks( new long[] {keyAndType, ((Long)value).longValue()} );
            }
        }
        else if ( value instanceof Double ) block.setValueBlocks( new long[] { keyId | (((long)PropertyType.DOUBLE.intValue()) << 24), Double.doubleToRawLongBits( ((Double)value).doubleValue() ) } );
        else if ( value instanceof Byte ) setSingleBlockValue( block, keyId, PropertyType.BYTE, ((Byte)value).longValue() );
        else if ( value instanceof Character ) setSingleBlockValue( block, keyId, PropertyType.CHAR, ((Character)value).charValue() );
        else if ( value instanceof Short ) setSingleBlockValue( block, keyId, PropertyType.SHORT, ((Short)value).longValue() );
        else if ( value.getClass().isArray() )
        {   // Try short array first, i.e. inlined in the property block
            if ( ShortArray.encode( keyId, value, block, PropertyType.getPayloadSize() ) ) return;

            // Fall back to dynamic array store
            long arrayBlockId = nextArrayBlockId();
            setSingleBlockValue( block, keyId, PropertyType.ARRAY, arrayBlockId );
            Collection<DynamicRecord> arrayRecords = allocateArrayRecords( arrayBlockId, value );
            for ( DynamicRecord valueRecord : arrayRecords )
            {
                valueRecord.setType( PropertyType.ARRAY.intValue() );
                block.addValueRecord( valueRecord );
            }
        }
        else
        {
            throw new IllegalArgumentException( "Unknown property type on: "
                + value + ", " + value.getClass() );
        }
    }

    private void setSingleBlockValue( PropertyBlock block, int keyId, PropertyType type, long longValue )
    {
        block.setSingleBlock( keyId | (((long) type.intValue()) << 24)
                | (longValue << 28) );
    }

    public static byte[] encodeString( String string )
    {
        return UTF8.encode( string );
    }

    public Object getStringFor( PropertyBlock propertyBlock )
    {
        return getStringFor( stringPropertyStore, propertyBlock );
    }

    public static Object getStringFor( AbstractDynamicStore store, PropertyBlock propertyBlock )
    {
        return getStringFor( store, propertyBlock.getSingleValueLong(), propertyBlock.getValueRecords() );
    }

    public static Object getStringFor( AbstractDynamicStore store, long startRecord, Collection<DynamicRecord> dynamicRecords )
    {
        byte[] source = readFullByteArray( startRecord, dynamicRecords, store );
        return getStringFor( source );
    }

    public static Object getStringFor( byte[] byteArray )
    {
        return UTF8.decode( byteArray );
    }

    public Object getArrayFor( PropertyBlock propertyBlock )
    {
        assert !propertyBlock.isLight();
        return getArrayFor( propertyBlock.getSingleValueLong(), propertyBlock.getValueRecords(), arrayPropertyStore );
    }

    public static Object getArrayFor( long startRecord, Iterable<DynamicRecord> records,
            DynamicArrayStore arrayPropertyStore )
    {
        return arrayPropertyStore.getRightArray(
                readFullByteArray( startRecord, records, arrayPropertyStore ) );
    }

    public static byte[] readFullByteArray( long startRecord, Iterable<DynamicRecord> records,
            AbstractDynamicStore store )
    {
        long recordToFind = startRecord;
        Map<Long,DynamicRecord> recordsMap = new HashMap<Long,DynamicRecord>();
        for ( DynamicRecord record : records )
        {
            recordsMap.put( record.getId(), record );
        }
        List<byte[]> byteList = new LinkedList<byte[]>();
        int totalSize = 0;
        while ( recordToFind != Record.NO_NEXT_BLOCK.intValue() )
        {
            DynamicRecord record = recordsMap.get( recordToFind );
            if ( record.isLight() )
            {
                store.makeHeavy( record );
            }
            assert record.getData().length > 0;
            // assert ( ( record.getData().length == ( DEFAULT_DATA_BLOCK_SIZE -
            // AbstractDynamicStore.BLOCK_HEADER_SIZE ) ) && (
            // record.getNextBlock() != Record.NO_NEXT_BLOCK.intValue() ) )
            // || ( ( record.getData().length < ( DEFAULT_DATA_BLOCK_SIZE -
            // AbstractDynamicStore.BLOCK_HEADER_SIZE ) ) && (
            // record.getNextBlock() == Record.NO_NEXT_BLOCK.intValue() ) );
            ByteBuffer buf = ByteBuffer.wrap( record.getData() );
            byte[] bytes = new byte[record.getData().length];
            totalSize += bytes.length;
            buf.get( bytes );
            byteList.add( bytes );
            recordToFind = record.getNextBlock();
        }
        byte[] bArray = new byte[totalSize];
        int offset = 0;
        for ( byte[] currentArray : byteList )
        {
            System.arraycopy( currentArray, 0, bArray, offset,
                currentArray.length );
            offset += currentArray.length;
        }
        assert bArray.length > 0;
        return bArray;
    }

    @Override
    public List<WindowPoolStats> getAllWindowPoolStats()
    {
        List<WindowPoolStats> list = new ArrayList<WindowPoolStats>();
        list.add( stringPropertyStore.getWindowPoolStats() );
        list.add( arrayPropertyStore.getWindowPoolStats() );
        list.add( getWindowPoolStats() );
        return list;
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
    protected boolean isRecordInUse( ByteBuffer buffer )
    {
        // TODO: The next line is an ugly hack, but works.
        Buffer fromByteBuffer = new Buffer( null, buffer );
        return getRecordFromBuffer( 0, fromByteBuffer ).inUse();
    }

    @Override
    public void logVersions( StringLogger msgLog )
    {
        super.logVersions( msgLog );
        propertyIndexStore.logVersions( msgLog );
        stringPropertyStore.logVersions( msgLog );
        arrayPropertyStore.logVersions( msgLog );
    }

    @Override
    public void logIdUsage( StringLogger logger )
    {
        NeoStore.logIdUsage( logger, this );
        propertyIndexStore.logIdUsage( logger );
        stringPropertyStore.logIdUsage( logger );
        arrayPropertyStore.logIdUsage( logger );
    }
    
    @Override
    public String toString()
    {
        return super.toString() + "[blocksPerRecord:" + PropertyType.getPayloadSizeLongs() + "]";
    }
}