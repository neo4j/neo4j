/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the property store. This implementation has two dynamic
 * stores. One used to store keys and another for string property values.
 */
public class PropertyStore extends AbstractStore implements Store
{
    // store version, each store ends with this string (byte encoded)
    private static final String VERSION = "PropertyStore v0.9.5";

    // record header size
    // in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
    // prev_prop_id(int)+next_prop_id(int)
    private static final int RECORD_SIZE = 25;

    private static final int STRING_STORE_BLOCK_SIZE = 120;

    private DynamicStringStore stringPropertyStore;
    private PropertyIndexStore propertyIndexStore;
    private DynamicArrayStore arrayPropertyStore;

    /**
     * See {@link AbstractStore#AbstractStore(String, Map)}
     */
    public PropertyStore( String fileName, Map<?,?> config )
    {
        super( fileName, config );
    }

    /**
     * See {@link AbstractStore#AbstractStore(String)}
     */
    public PropertyStore( String fileName )
    {
        super( fileName );
    }

    @Override
    protected void initStorage()
    {
        stringPropertyStore = new DynamicStringStore( getStorageFileName()
            + ".strings", getConfig() );
        propertyIndexStore = new PropertyIndexStore( getStorageFileName()
            + ".index", getConfig() );
        File arrayStoreFile = new File( getStorageFileName() + ".arrays" );
        // old store, create array store
        if ( !arrayStoreFile.exists() )
        {
            DynamicArrayStore.createStore( getStorageFileName() + ".arrays",
                STRING_STORE_BLOCK_SIZE );
        }
        arrayPropertyStore = new DynamicArrayStore( getStorageFileName()
            + ".arrays", getConfig() );
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
        stringPropertyStore.close();
        stringPropertyStore = null;
        propertyIndexStore.close();
        propertyIndexStore = null;
        arrayPropertyStore.close();
        arrayPropertyStore = null;
    }

    @Override
    public void flushAll()
    {
        stringPropertyStore.flushAll();
        propertyIndexStore.flushAll();
        arrayPropertyStore.flushAll();
        super.flushAll();
    }

    public String getTypeAndVersionDescriptor()
    {
        return VERSION;
    }

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
    public static void createStore( String fileName )
    {
        createEmptyStore( fileName, VERSION );
        DynamicStringStore.createStore( fileName + ".strings",
            STRING_STORE_BLOCK_SIZE );
        PropertyIndexStore.createStore( fileName + ".index" );
        DynamicArrayStore.createStore( fileName + ".arrays",
            STRING_STORE_BLOCK_SIZE );
    }

    private int nextStringBlockId()
    {
        return stringPropertyStore.nextBlockId();
    }

    public void freeStringBlockId( int blockId )
    {
        stringPropertyStore.freeBlockId( blockId );
    }

    private int nextArrayBlockId()
    {
        return arrayPropertyStore.nextBlockId();
    }

    public void freeArrayBlockId( int blockId )
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
        if ( !record.isLight() )
        {
            for ( DynamicRecord valueRecord : record.getValueRecords() )
            {
                if ( valueRecord.getType() == PropertyType.STRING.intValue() )
                {
                    stringPropertyStore.updateRecord( valueRecord );
                }
                else if ( valueRecord.getType() == 
                    PropertyType.ARRAY.intValue() )
                {
                    arrayPropertyStore.updateRecord( valueRecord );
                }
                else
                {
                    throw new RuntimeException( "Unknown dynamic record" );
                }
            }
        }
    }

    // in_use(byte)+type(int)+key_blockId(int)+prop_blockId(long)+
    // prev_prop_id(int)+next_prop_id(int)

    private void updateRecord( PropertyRecord record, PersistenceWindow window )
    {
        int id = record.getId();
        Buffer buffer = window.getOffsettedBuffer( id );
        if ( record.inUse() )
        {
            buffer.put( Record.IN_USE.byteValue() ).putInt(
                record.getType().intValue() ).putInt( record.getKeyIndexId() )
                .putLong( record.getPropBlock() ).putInt( record.getPrevProp() )
                .putInt( record.getNextProp() );
        }
        else
        {
            buffer.put( Record.NOT_IN_USE.byteValue() );
            if ( !isInRecoveryMode() )
            {
                freeId( id );
            }
        }
    }

    public PropertyRecord getLightRecord( int id )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            PropertyRecord record = getRecord( id, window );
            record.setIsLight( true );
            return record;
        }
        finally
        {
            releaseWindow( window );
        }
    }

    public void makeHeavy( PropertyRecord record )
    {
        record.setIsLight( false );
        if ( record.getType() == PropertyType.STRING )
        {
            Collection<DynamicRecord> stringRecords = 
                stringPropertyStore.getLightRecords( 
                    (int) record.getPropBlock() );
            for ( DynamicRecord stringRecord : stringRecords )
            {
                stringRecord.setType( PropertyType.STRING.intValue() );
                record.addValueRecord( stringRecord );
            }
        }
        else if ( record.getType() == PropertyType.ARRAY )
        {
            Collection<DynamicRecord> arrayRecords = 
                arrayPropertyStore.getLightRecords( 
                    (int) record.getPropBlock() );
            for ( DynamicRecord arrayRecord : arrayRecords )
            {
                arrayRecord.setType( PropertyType.ARRAY.intValue() );
                record.addValueRecord( arrayRecord );
            }
        }
    }

    public PropertyRecord getRecord( int id )
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
        if ( record.getType() == PropertyType.STRING )
        {
            Collection<DynamicRecord> stringRecords = 
                stringPropertyStore.getLightRecords( 
                    (int) record.getPropBlock() );
            record.setIsLight( false );
            for ( DynamicRecord stringRecord : stringRecords )
            {
                stringRecord.setType( PropertyType.STRING.intValue() );
                record.addValueRecord( stringRecord );
            }
        }
        else if ( record.getType() == PropertyType.ARRAY )
        {
            Collection<DynamicRecord> arrayRecords = 
                arrayPropertyStore.getLightRecords( 
                    (int) record.getPropBlock() );
            record.setIsLight( false );
            for ( DynamicRecord arrayRecord : arrayRecords )
            {
                arrayRecord.setType( PropertyType.ARRAY.intValue() );
                record.addValueRecord( arrayRecord );
            }
        }
        return record;
    }

    private PropertyRecord getRecord( int id, PersistenceWindow window )
    {
        Buffer buffer = window.getOffsettedBuffer( id );
        if ( buffer.get() != Record.IN_USE.byteValue() )
        {
            throw new StoreFailureException( "Record[" + id + "] not in use" );
        }
        PropertyRecord record = new PropertyRecord( id );
        record.setType( getEnumType( buffer.getInt() ) );
        record.setInUse( true );
        record.setKeyIndexId( buffer.getInt() );
        record.setPropBlock( buffer.getLong() );
        record.setPrevProp( buffer.getInt() );
        record.setNextProp( buffer.getInt() );
        return record;
    }

    private PropertyType getEnumType( int type )
    {
        switch ( type )
        {
            case 1:
                return PropertyType.INT;
            case 2:
                return PropertyType.STRING;
            case 3:
                return PropertyType.BOOL;
            case 4:
                return PropertyType.DOUBLE;
            case 5:
                return PropertyType.FLOAT;
            case 6:
                return PropertyType.LONG;
            case 7:
                return PropertyType.BYTE;
            case 8:
                return PropertyType.CHAR;
            case 9:
                return PropertyType.ARRAY;
            case 10:
                return PropertyType.SHORT;
            default:
                throw new StoreFailureException( "Unknown enum type:" + type );
        }
    }
    
    public Object getValue( PropertyRecord propertyRecord )
    {
        PropertyType type = propertyRecord.getType();
        if ( type == PropertyType.INT )
        {
            return (int) propertyRecord.getPropBlock();
        }
        if ( type == PropertyType.STRING )
        {
            return getStringFor( propertyRecord );
        }
        if ( type == PropertyType.BOOL )
        {
            if ( propertyRecord.getPropBlock() == 1 )
            {
                return Boolean.valueOf( true );
            }
            return Boolean.valueOf( false );
        }
        if ( type == PropertyType.DOUBLE )
        {
            return new Double( Double.longBitsToDouble( 
                propertyRecord.getPropBlock() ) );
        }
        if ( type == PropertyType.FLOAT )
        {
            return new Float( Float.intBitsToFloat( 
                (int) propertyRecord.getPropBlock() ) );
        }
        if ( type == PropertyType.LONG )
        {
            return propertyRecord.getPropBlock();
        }
        if ( type == PropertyType.BYTE )
        {
            return (byte) propertyRecord.getPropBlock();
        }
        if ( type == PropertyType.CHAR )
        {
            return (char) propertyRecord.getPropBlock();
        }
        if ( type == PropertyType.ARRAY )
        {
            return getArrayFor( propertyRecord );
        }
        if ( type == PropertyType.SHORT )
        {
            return (short) propertyRecord.getPropBlock();
        }
        throw new RuntimeException( "Unknown type: " + type );
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
    
    private Collection<DynamicRecord> allocateStringRecords( int valueBlockId,
        char[] chars )
    {
        return stringPropertyStore.allocateRecords( valueBlockId, chars );
    }

    private Collection<DynamicRecord> allocateArrayRecords( int valueBlockId,
        Object array )
    {
        return arrayPropertyStore.allocateRecords( valueBlockId, array );
    }

    public void encodeValue( PropertyRecord record, Object value )
    {
        if ( value instanceof String )
        {
            int stringBlockId = nextStringBlockId();
            record.setPropBlock( stringBlockId );
            String string = (String) value;
            int length = string.length();
            char[] chars = new char[length];
            string.getChars( 0, length, chars, 0 );
            Collection<DynamicRecord> valueRecords = allocateStringRecords(
                stringBlockId, chars );
            for ( DynamicRecord valueRecord : valueRecords )
            {
                valueRecord.setType( PropertyType.STRING.intValue() );
                record.addValueRecord( valueRecord );
            }
            record.setType( PropertyType.STRING );
        }
        else if ( value instanceof Integer )
        {
            record.setPropBlock( ((Integer) value).intValue() );
            record.setType( PropertyType.INT );
        }
        else if ( value instanceof Boolean )
        {
            record.setPropBlock( (((Boolean) value).booleanValue() ? 1 : 0) );
            record.setType( PropertyType.BOOL );
        }
        else if ( value instanceof Float )
        {
            record.setPropBlock( Float.floatToRawIntBits( ((Float) value)
                .floatValue() ) );
            record.setType( PropertyType.FLOAT );
        }
        else if ( value instanceof Long )
        {
            record.setPropBlock( ((Long) value).longValue() );
            record.setType( PropertyType.LONG );
        }
        else if ( value instanceof Double )
        {
            record.setPropBlock( Double.doubleToRawLongBits( ((Double) value)
                .doubleValue() ) );
            record.setType( PropertyType.DOUBLE );
        }
        else if ( value instanceof Byte )
        {
            record.setPropBlock( ((Byte) value).byteValue() );
            record.setType( PropertyType.BYTE );
        }
        else if ( value instanceof Character )
        {
            record.setPropBlock( ((Character) value).charValue() );
            record.setType( PropertyType.CHAR );
        }
        else if ( value.getClass().isArray() )
        {
            int arrayBlockId = nextArrayBlockId();
            record.setPropBlock( arrayBlockId );
            Collection<DynamicRecord> arrayRecords = allocateArrayRecords(
                arrayBlockId, value );
            for ( DynamicRecord valueRecord : arrayRecords )
            {
                valueRecord.setType( PropertyType.ARRAY.intValue() );
                record.addValueRecord( valueRecord );
            }
            record.setType( PropertyType.ARRAY );
        }
        else if ( value instanceof Short )
        {
            record.setPropBlock( ((Short) value).shortValue() );
            record.setType( PropertyType.SHORT );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown property type on: "
                + value );
        }
    }

    public Object getStringFor( PropertyRecord propRecord )
    {
        int recordToFind = (int) propRecord.getPropBlock();
        Iterator<DynamicRecord> records = 
            propRecord.getValueRecords().iterator();
        List<char[]> charList = new LinkedList<char[]>();
        int totalSize = 0;
        while ( recordToFind != Record.NO_NEXT_BLOCK.intValue() && 
            records.hasNext() )
        {
            DynamicRecord record = records.next();
            if ( record.getId() == recordToFind )
            {
                if ( record.isLight() )
                {
                    stringPropertyStore.makeHeavy( record );
                }
                if ( !record.isCharData() )
                {
                    ByteBuffer buf = ByteBuffer.wrap( record.getData() );
                    char[] chars = new char[record.getData().length / 2];
                    totalSize += chars.length;
                    buf.asCharBuffer().get( chars );
                    charList.add( chars );
                }
                else
                {
                    charList.add( record.getDataAsChar() );
                }
                recordToFind = record.getNextBlock();
                // TODO: make opti here, high chance next is right one
                records = propRecord.getValueRecords().iterator();
            }
        }
        StringBuffer buf = new StringBuffer();
        for ( char[] str : charList )
        {
            buf.append( str );
        }
        return buf.toString();
    }

    public Object getArrayFor( PropertyRecord propertyRecord )
    {
        int recordToFind = (int) propertyRecord.getPropBlock();
        Iterator<DynamicRecord> records = 
            propertyRecord.getValueRecords().iterator();
        List<byte[]> byteList = new LinkedList<byte[]>();
        int totalSize = 0;
        while ( recordToFind != Record.NO_NEXT_BLOCK.intValue() && 
            records.hasNext() )
        {
            DynamicRecord record = records.next();
            if ( record.getId() == recordToFind )
            {
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
                    throw new RuntimeException( "Assert" );
                }
                recordToFind = record.getNextBlock();
                // TODO: make opti here, high chance next is right one
                records = propertyRecord.getValueRecords().iterator();
            }
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
        if ( version.equals( "PropertyStore v0.9.3" ) )
        {
            rebuildIdGenerator();
            closeIdGenerator();
            return true;
        }
        throw new RuntimeException( "Unknown store version " + version  + 
            " Please make sure you are not running old Neo4j kernel " + 
            " towards a store that has been created by newer version " + 
            " of Neo4j." );
    }
}