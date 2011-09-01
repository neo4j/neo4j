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

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.util.Bits;

/**
 * Implementation of the property store. This implementation has two dynamic
 * stores. One used to store keys and another for string property values.
 */
public class PropertyStore extends AbstractStore implements Store
{
    public static final int DEFAULT_DATA_BLOCK_SIZE = 120;
    public static final int DEFAULT_PAYLOAD_SIZE = 32;

    // store version, each store ends with this string (byte encoded)
    private static final String VERSION = "PropertyStore v0.9.9";

    public static final int RECORD_SIZE = 1/*next and prev high bits*/
    + 4/*next*/
    + 4/*prev*/
    + DEFAULT_PAYLOAD_SIZE /*property blocks*/;
    // = 41

    private DynamicStringStore stringPropertyStore;
    private PropertyIndexStore propertyIndexStore;
    private DynamicArrayStore arrayPropertyStore;

    /**
     * See {@link AbstractStore#AbstractStore(String, Map)}
     */
    public PropertyStore( String fileName, Map<?,?> config )
    {
        super( fileName, config, IdType.PROPERTY );
    }

    /**
     * See {@link AbstractStore#AbstractStore(String)}
     */
//    public PropertyStore( String fileName )
//    {
//        super( fileName );
//    }

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
    public String getTypeAndVersionDescriptor()
    {
        return VERSION;
    }

    @Override
    public int getRecordSize()
    {
        return RECORD_SIZE;
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

        createEmptyStore( fileName, VERSION, idGeneratorFactory );
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

    private void updateRecord( PropertyRecord record, PersistenceWindow window )
    {
        long id = record.getId();
        Buffer buffer = window.getOffsettedBuffer( id );
        Bits bits = Bits.bits( RECORD_SIZE );
        if ( record.inUse() )
        {

            short prevModifier = record.getPrevProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (short)((record.getPrevProp() & 0xF00000000L) >> 32);
            short nextModifier = record.getNextProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (short)((record.getNextProp() & 0xF00000000L) >> 32);
            bits.or( prevModifier, 0xF ).shiftLeft( 4 ).or( nextModifier, 0xF );
            bits.shiftLeft( 32 ).or( (int)record.getPrevProp() );
            bits.shiftLeft( 32 ).or( (int)record.getNextProp() );

            int longsPushed = 0;
            for ( PropertyBlock block : record.getPropertyBlocks() )
            {
                if ( !block.inUse() )
                {
                    bits.shiftLeft( 64 ).or( 0 );
                    longsPushed++;
                    continue;
                }
                for ( long propBlockValue : block.getValueBlocks() )
                {
                    longsPushed++;
                    bits.shiftLeft( 64 ).or( propBlockValue );
                }
                if ( !block.isLight() )
                {
                    for ( DynamicRecord valueRecord : block.getValueRecords() )
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
                            throw new InvalidRecordException(
                                    "Unknown dynamic record" );
                        }
                    }
                }
            }
            while ( longsPushed < PropertyType.getPayloadSizeLongs() )
            {
                bits.shiftLeft( 64 ).or( 0l );
                longsPushed++;
            }
        }
        else
        {
            if ( !isInRecoveryMode() )
            {
                freeId( id );
            }
        }
        bits.apply( buffer );
    }

    public PropertyRecord getLightRecord( long id )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            PropertyRecord record = getRecord( id, window );
            return record;
        }
        finally
        {
            releaseWindow( window );
        }
    }

    public void makeHeavy( PropertyBlock record )
    {
        if ( record.getType() == PropertyType.STRING )
        {
            Collection<DynamicRecord> stringRecords = stringPropertyStore.getLightRecords( record.getSingleValueBlock() & 0xFFFFFFFFFL );
            for ( DynamicRecord stringRecord : stringRecords )
            {
                stringRecord.setType( PropertyType.STRING.intValue() );
                record.addValueRecord( stringRecord );
            }
        }
        else if ( record.getType() == PropertyType.ARRAY )
        {
            Collection<DynamicRecord> arrayRecords = arrayPropertyStore.getLightRecords( record.getSingleValueBlock() & 0xFFFFFFFFFL );
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
            record = getRecord( id, window );
        }
        finally
        {
            releaseWindow( window );
        }
        for ( PropertyBlock block : record.getPropertyBlocks() )
        {
            if ( block.getType() == PropertyType.STRING )
            {
                Collection<DynamicRecord> stringRecords = stringPropertyStore.getLightRecords( block.getValueBlocks()[0] & 0xFFFFFFFFFL );
                for ( DynamicRecord stringRecord : stringRecords )
                {
                    stringRecord.setType( PropertyType.STRING.intValue() );
                    block.addValueRecord( stringRecord );
                }
            }
            else if ( block.getType() == PropertyType.ARRAY )
            {
                Collection<DynamicRecord> arrayRecords = arrayPropertyStore.getLightRecords( block.getValueBlocks()[0] & 0xFFFFFFFFFL );
                for ( DynamicRecord arrayRecord : arrayRecords )
                {
                    arrayRecord.setType( PropertyType.ARRAY.intValue() );
                    block.addValueRecord( arrayRecord );
                }
            }
        }
        return record;
    }

    private PropertyRecord getRecord( long id, PersistenceWindow window )
    {
        Buffer buffer = window.getOffsettedBuffer( id );

        Bits bits = Bits.bits( RECORD_SIZE );
        bits.read( buffer );
        bits.pullLeftLong( 8 * 7 );

        PropertyRecord record = new PropertyRecord( id );
        record.setInUse( true );

        long prevMod = (long) bits.pullLeftByte( 4 ) << 32;
        long nextMod = (long) bits.pullLeftByte( 4 ) << 32;
        long prevProp = bits.pullLeftLong( 32 );
        long nextProp = bits.pullLeftLong( 32 );
        record.setPrevProp( longFromIntAndMod( prevProp, prevMod ) );
        record.setNextProp( longFromIntAndMod( nextProp, nextMod ) );

        boolean someBlockInUse = false;
        while(bits.getLongs()[0] != 0)
        {
            PropertyBlock newBlock = getPropertyBlock(bits);
            if ( newBlock.inUse() )
            {
                someBlockInUse = true;
                record.addPropertyBlock( newBlock );
                for ( int i = 0; i < newBlock.getType().getSizeInLongs(); i++ )
                {
                    bits.pullLeftLong();
                }
            }
            else
            {
                // We assume that storage is defragged
                break;
            }
        }
        if ( !someBlockInUse )
        {
            throw new InvalidRecordException( "Record[" + id + "] not in use" );
        }
        return record;
    }

    /*
     * It is assumed that the argument does hold a property block - all zeros is
     * a valid (not in use) block, so even if the Bits object has been exhausted a
     * result is returned, that has inUse() return false. Also, the argument is not
     * touched.
     */
    private static PropertyBlock getPropertyBlock( Bits fromBits )
    {
        PropertyBlock toReturn = new PropertyBlock();

        long header = fromBits.getLongs()[0];
        PropertyType type = PropertyType.getPropertyType( header, true );
        if ( type == null )
        {
            toReturn.setInUse( false );
            return toReturn;
        }
        toReturn.setInUse( true );
        long[] blockData = new long[type.getSizeInLongs()];
        blockData[0] = header; // we already have that;
        for ( int i = 1; i < type.getSizeInLongs(); i++ )
        {
            blockData[i] = fromBits.getLongs()[i];
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

    private Collection<DynamicRecord> allocateStringRecords( long valueBlockId,
        char[] chars )
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
        {
            String string = (String) value;
            if ( LongerShortString.encode( keyId, string, block,
                    PropertyType.getPayloadSize() ) ) return;

            Bits bits = bits32WithKeyAndType( keyId, PropertyType.STRING );
            long stringBlockId = nextStringBlockId();
            bits.or( stringBlockId, 0xFFFFFFFFFL );
            block.setSingleBlock( bits.getLongs()[0] );
            int length = string.length();
            char[] chars = new char[length];
            string.getChars( 0, length, chars, 0 );
            Collection<DynamicRecord> valueRecords = allocateStringRecords(
                stringBlockId, chars );
            for ( DynamicRecord valueRecord : valueRecords )
            {
                valueRecord.setType( PropertyType.STRING.intValue() );
                block.addValueRecord( valueRecord );
            }
        }
        else if ( value instanceof Integer )
        {
            Bits bits = bits32WithKeyAndType( keyId, PropertyType.INT ).or( ((Integer)value).intValue() );
            block.setSingleBlock( bits.getLongs()[0] );
        }
        else if ( value instanceof Boolean )
        {
            Bits bits = bits32WithKeyAndType( keyId, PropertyType.BOOL ).or( ((Boolean)value).booleanValue() ? 1 : 0, 0x1 );
            block.setSingleBlock( bits.getLongs()[0] );
        }
        else if ( value instanceof Float )
        {
            Bits bits = bits32WithKeyAndType( keyId, PropertyType.FLOAT ).or( Float.floatToRawIntBits( ((Float) value).floatValue() ) );
            block.setSingleBlock( bits.getLongs()[0] );
        }
        else if ( value instanceof Long )
        {
            Bits bits = bits64WithKeyAndType( keyId, PropertyType.LONG ).or( ((Long)value).longValue() );
            block.setValueBlocks( bits.getLongs() );
        }
        else if ( value instanceof Double )
        {
            Bits bits = bits64WithKeyAndType( keyId, PropertyType.DOUBLE ).or( Double.doubleToRawLongBits( ((Double)value).doubleValue() ) );
            block.setValueBlocks( bits.getLongs() );
        }
        else if ( value instanceof Byte )
        {
            Bits bits = bits32WithKeyAndType( keyId, PropertyType.BYTE ).or( ((Byte)value).byteValue() );
            block.setSingleBlock( bits.getLongs()[0] );
        }
        else if ( value instanceof Character )
        {
            Bits bits = bits32WithKeyAndType( keyId, PropertyType.CHAR ).or( ((Character)value).charValue() );
            block.setSingleBlock( bits.getLongs()[0] );
        }
        else if ( value instanceof Short )
        {
            Bits bits = bits32WithKeyAndType( keyId, PropertyType.SHORT ).or( ((Short)value).shortValue() );
            block.setSingleBlock( bits.getLongs()[0] );
        }
        else if ( value.getClass().isArray() )
        {
            if ( ShortArray.encode( keyId, value, block, DEFAULT_PAYLOAD_SIZE ) ) return;
            long arrayBlockId = nextArrayBlockId();
            Bits bits = bits32WithKeyAndType( keyId, PropertyType.ARRAY ).or( arrayBlockId, 0xFFFFFFFFFL );
            block.setSingleBlock( bits.getLongs()[0] );
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

    // TODO Assume only one prop per record for now
    private Bits bits32WithKeyAndType( int keyId, PropertyType type )
    {
        return Bits.bits( 8 ).or( keyId, 0xFFFFFF ).shiftLeft( 4 ).or(
                type.intValue(), 0xF ).shiftLeft( 36 );
    }

    // TODO Assume only one prop per record for now
    private Bits bits64WithKeyAndType( int keyId, PropertyType type )
    {
        return Bits.bits( 16 ).or( keyId, 0xFFFFFF ).shiftLeft( 4 ).or(
                type.intValue(), 0xF ).shiftLeft( 36 + 64 );
    }

    public Object getStringFor( PropertyBlock propRecord )
    {
        long recordToFind = propRecord.getSingleValueBlock() & 0xFFFFFFFFFL;
        Map<Long,DynamicRecord> recordsMap = new HashMap<Long,DynamicRecord>();
        for ( DynamicRecord record : propRecord.getValueRecords() )
        {
            recordsMap.put( record.getId(), record );
        }
        List<char[]> charList = new LinkedList<char[]>();
//        int totalSize = 0;
        while ( recordToFind != Record.NO_NEXT_BLOCK.intValue() )
        {
            DynamicRecord record = recordsMap.get( recordToFind );
            if ( record.isLight() )
            {
                stringPropertyStore.makeHeavy( record );
            }
            if ( !record.isCharData() )
            {
                ByteBuffer buf = ByteBuffer.wrap( record.getData() );
                char[] chars = new char[record.getData().length / 2];
//                totalSize += chars.length;
                buf.asCharBuffer().get( chars );
                charList.add( chars );
            }
            else
            {
                charList.add( record.getDataAsChar() );
            }
            recordToFind = record.getNextBlock();
        }
        StringBuffer buf = new StringBuffer();
        for ( char[] str : charList )
        {
            buf.append( str );
        }
        return buf.toString();
    }

    public Object getArrayFor( PropertyBlock propertyBlock )
    {
        return getArrayFor( propertyBlock.getSingleValueBlock() & 0xFFFFFFFFFL,
                propertyBlock.getValueRecords(), arrayPropertyStore );
    }

    public static Object getArrayFor( long startRecord, Iterable<DynamicRecord> records,
            DynamicArrayStore arrayPropertyStore )
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
                arrayPropertyStore.makeHeavy( record );
            }
            if ( !record.isCharData() )
            {
                ByteBuffer buf = ByteBuffer.wrap( record.getData() );
                byte[] bytes = new byte[record.getData().length];
                totalSize += bytes.length;
                buf.get( bytes );
                byteList.add( bytes );
            }
            else
            {
                throw new InvalidRecordException(
                    "Expected byte data on record " + record );
            }
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
        return arrayPropertyStore.getRightArray( bArray );
    }

    @Override
    protected boolean versionFound( String version )
    {
        if ( !version.startsWith( "PropertyStore" ) )
        {
            // non clean shutdown, need to do recover with right neo
            return false;
        }
//        if ( version.equals( "PropertyStore v0.9.3" ) )
//        {
//            rebuildIdGenerator();
//            closeIdGenerator();
//            return true;
//        }
        if ( version.equals( "PropertyStore v0.9.5" ) )
        {
            return true;
        }
        throw new IllegalStoreVersionException( "Store version [" + version  +
            "]. Please make sure you are not running old Neo4j kernel " +
            " towards a store that has been created by newer version " +
            " of Neo4j." );
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
}