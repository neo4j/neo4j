/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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
package org.neo4j.impl.nioneo.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.neo4j.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.impl.nioneo.store.DynamicRecord;
import org.neo4j.impl.nioneo.store.NeoStore;
import org.neo4j.impl.nioneo.store.PropertyRecord;
import org.neo4j.impl.nioneo.store.PropertyStore;
import org.neo4j.impl.nioneo.store.PropertyType;
import org.neo4j.impl.nioneo.store.Record;
import org.neo4j.impl.nioneo.xa.NeoStoreXaDataSource;

public class AdminStore
{
    private static void usage()
    {
        System.out.println( "Nothing to do" );
        System.out.println( "Usage: AdminStore [options...]" );
        System.out.println( "-h, --help\t\t\t" + "Displays this information." );
        System.out.println( "-c, --create [filename]\t\t"
            + "Creates a new store." );
        System.out.println( "--fsck [filename]\t\t"
            + "Check and repair a store" );
    }

    public static void main( String args[] ) throws IOException,
        InstantiationException
    {
        if ( args.length == 0 )
        {
            usage();
            System.exit( 1 );
        }
        performRequest( args );
    }

    private static void performRequest( String[] args ) throws IOException,
        InstantiationException
    {
        for ( int i = 0; i < args.length; i++ )
        {
            if ( args[i].equals( "-c" ) || args[i].equals( "--create" ) )
            {
                createStore( args[++i] );
            }
            else if ( args[i].equals( "--open" ) )
            {
                Properties properties = new Properties();
                FileInputStream inputStream = new FileInputStream( args[++i] );
                try
                {
                    properties.load( inputStream );
                    new NeoStoreXaDataSource( properties ).close();
                }
                finally
                {
                    inputStream.close();
                }
            }
            else if ( args[i].equals( "--dump-rel-types" ) )
            {
                dumpRelTypes( args[++i] );
            }
            else if ( args[i].equals( "--convert-rel-types" ) )
            {
                convertRelTypes( args[++i] );
            }
            else if ( args[i].equals( "--fix-same-index" ) )
            {
                fixMultiIndex( args[++i] );
            }
            else if ( args[i].equals( "--fsck" ) )
            {
                fsckStore( args[++i] );
            }
            else
            {
                usage();
                return;
            }
        }
    }

    private static class DynamicStringStore extends AbstractDynamicStore
    {
        private static final String VERSION = "StringPropertyStore v0.9.3";

        public DynamicStringStore( String fileName, Map<?,?> config )
        {
            super( fileName, config );
        }

        public DynamicStringStore( String fileName )
        {
            super( fileName );
        }

        public String getTypeAndVersionDescriptor()
        {
            return VERSION;
        }

        public static void createStore( String fileName, int blockSize )
        {
            createEmptyStore( fileName, blockSize, VERSION );
        }

        @Override
        public void rebuildIdGenerators()
        {
            rebuildIdGenerator();
        }
    }

    private static void fixMultiIndex( String fileName )
    {
        String storeName = fileName + ".propertystore.db";
        PropertyStore propStore = new PropertyStore( storeName );
        propStore.makeStoreOk();
        for ( int i = 0; i < propStore.getHighestPossibleIdInUse(); i++ )
        {
            PropertyRecord record;
            try
            {
                record = propStore.getLightRecord( i );
                if ( !record.inUse() || record.getPrevProp() != 
                    Record.NO_PREVIOUS_PROPERTY.intValue() )
                {
                    continue;
                }
            }
            catch ( Exception e )
            {
                // not in use
                continue;
            }
            Set<Integer> indexesFound = new HashSet<Integer>();
            do
            {
                int index = record.getKeyIndexId();
                if ( indexesFound.contains( index ) )
                {
                    System.out.println( "Found multi index: " + index );
                    if ( record.getType() == PropertyType.STRING )
                    {
                        propStore.makeHeavy( record );
                    }
                    if ( record.getPrevProp() == Record.NO_PREVIOUS_PROPERTY
                        .intValue() )
                    {
                        throw new RuntimeException();
                    }
                    if ( record.getPrevProp() != 
                        Record.NO_PREVIOUS_PROPERTY.intValue() )
                    {
                        PropertyRecord prev = propStore.getLightRecord( 
                            record.getPrevProp() );
                        prev.setNextProp( record.getNextProp() );
                        propStore.updateRecord( prev );
                    }
                    if ( record.getNextProp() != 
                        Record.NO_NEXT_PROPERTY.intValue() )
                    {
                        PropertyRecord next = propStore.getLightRecord( 
                            record.getNextProp() );
                        next.setPrevProp( record.getPrevProp() );
                        propStore.updateRecord( next );
                    }
                    record.setInUse( false );
                    propStore.updateRecord( record );
                }
                else
                {
                    indexesFound.add( index );
                }
                int nextProp = record.getNextProp();
                if ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
                {
                    record = propStore.getLightRecord( nextProp );
                }
                else
                {
                    record = null;
                }
            }
            while ( record != null );
        }
        propStore.close();
    }

    private static void dumpRelTypes( String fileName ) throws IOException
    {
        String storeName = fileName + ".relationshiptypestore.db";
        File relTypeStore = new File( storeName );
        if ( !relTypeStore.exists() )
        {
            throw new IOException( "Couldn't find relationship type store "
                + storeName );
        }
        DynamicStringStore typeNameStore = new DynamicStringStore( storeName
            + ".names" );
        typeNameStore.makeStoreOk();
        // in_use(byte)+type_blockId(int)
        System.out.println( storeName );
        ByteBuffer buffer = ByteBuffer.allocate( 5 );
        FileChannel fileChannel = 
            new RandomAccessFile( storeName, "rw" ).getChannel();
        fileChannel.position( 0 );
        int i = 0;
        while ( fileChannel.read( buffer ) == 5 )
        {
            buffer.flip();
            byte inUse = buffer.get();
            int block = buffer.getInt();
            String name = "N/A";
            try
            {
                Collection<DynamicRecord> records = typeNameStore.getRecords(
                    block );
                name = getStringFor( records, block );
            }
            catch ( Exception e )
            {
            }
            System.out.println( "ID[" + i + "] use[" + inUse + "] blockId["
                + block + "] name[" + name + "]" );
            i++;
            buffer.clear();
        }
        typeNameStore.close();
    }

    private static void convertRelTypes( String fileName ) throws IOException
    {
        String storeName = fileName + ".relationshiptypestore.db";

        File relTypeStore = new File( storeName );
        if ( !relTypeStore.exists() )
        {
            throw new IOException( "Couldn't find relationship type store "
                + storeName );
        }
        DynamicStringStore typeNameStore = new DynamicStringStore( storeName
            + ".names" );
        typeNameStore.makeStoreOk();
        // in_use(byte)+type_blockId(int)
        System.out.println( storeName );
        ByteBuffer buffer = ByteBuffer.allocate( 5 );
        FileChannel fileChannel = new RandomAccessFile( storeName, "rw" )
            .getChannel();
        fileChannel.position( 0 );
        int i = 0;
        int reservedCount = 1;
        int lastOkPos = 0;
        while ( fileChannel.read( buffer ) == 5 )
        {
            buffer.flip();
            byte inUse = buffer.get();
            int block = buffer.getInt();
            String name = "RESERVED";
            if ( block == -1 )
            {
                name = name + reservedCount++;
            }
            else
            {
                try
                {
                    Collection<DynamicRecord> records = 
                        typeNameStore.getRecords( block );
                    name = getOldStringFor( records, block );
                    for ( DynamicRecord record : records )
                    {
                        record.setInUse( false );
                        typeNameStore.updateRecord( record );
                    }
                    int nextId = typeNameStore.nextBlockId();
                    char[] chars = new char[name.length()];
                    name.getChars( 0, name.length(), chars, 0 );
                    records = typeNameStore.allocateRecords( nextId, chars );
                    for ( DynamicRecord record : records )
                    {
                        typeNameStore.updateRecord( record );
                    }
                    buffer.flip();
                    buffer.putInt( nextId );
                    buffer.flip();
                    fileChannel.position( i * 5 + 1 );
                    fileChannel.write( buffer );
                    lastOkPos = i;
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                    name = null;
                }
            }
            System.out.println( "ID[" + i + "] use[" + inUse + "] blockId["
                + block + "] name[" + name + "]" );
            i++;
            buffer.clear();
        }
        typeNameStore.close();
        fileChannel.truncate( (lastOkPos + 1) * 5 );
    }

    public static void createStore( String fileName )
    {
        NeoStore.createStore( fileName );
    }

    private static String getStringFor( Collection<DynamicRecord> recordsCol,
        int startBlock )
    {
        int recordToFind = startBlock;
        Iterator<DynamicRecord> records = recordsCol.iterator();
        List<char[]> charList = new LinkedList<char[]>();
        int totalSize = 0;
        while ( recordToFind != Record.NO_NEXT_BLOCK.intValue()
            && records.hasNext() )
        {
            DynamicRecord record = records.next();
            if ( record.inUse() && record.getId() == recordToFind )
            {
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
                records = recordsCol.iterator();
            }
        }
        StringBuffer buf = new StringBuffer();
        for ( char[] str : charList )
        {
            buf.append( str );
        }
        return buf.toString();
    }

    private static String getOldStringFor(
        Collection<DynamicRecord> recordsCol, int startBlock )
    {
        int recordToFind = startBlock;
        Iterator<DynamicRecord> records = recordsCol.iterator();
        List<byte[]> byteList = new LinkedList<byte[]>();
        int totalSize = 0;
        while ( recordToFind != Record.NO_NEXT_BLOCK.intValue()
            && records.hasNext() )
        {
            DynamicRecord record = records.next();
            if ( record.inUse() && record.getId() == recordToFind )
            {
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
                    throw new RuntimeException();
                    // charList.add( record.getDataAsChar() );
                }
                recordToFind = record.getNextBlock();
                // TODO: make opti here, high chance next is right one
                records = recordsCol.iterator();
            }
        }
        byte[] allBytes = new byte[totalSize];
        int position = 0;
        for ( byte[] bytes : byteList )
        {
            System.arraycopy( bytes, 0, allBytes, position, bytes.length );
            position += bytes.length;
        }
        return new String( allBytes );
    }

    public static void fsckStore( String fileName ) throws IOException
    {
        File neoStore = new File( fileName );
        if ( !neoStore.exists() )
        {
            throw new IOException( "No such neostore " + fileName );
        }
        Set<Integer> relTypeSet = checkRelTypeStore( fileName
            + ".relationshiptypestore.db" );
        Set<Integer> propertyIndexSet = checkPropertyIndexStore( fileName
            + ".propertystore.db.index" );
        Set<Integer> propertySet = checkPropertyStore( fileName
            + ".propertystore.db", propertyIndexSet );
        Set<Integer> nodeSet = checkNodeStore( fileName + ".nodestore.db",
            propertySet );
        checkRelationshipStore( fileName + ".relationshipstore.db",
            propertySet, relTypeSet, nodeSet );
        if ( !propertySet.isEmpty() )
        {
            System.out.println( "Stray properties found : "
                + propertySet.size() );
        }
        if ( !nodeSet.isEmpty() )
        {
            throw new IOException( "Node(s) with set relationship(s) found "
                + "but relationships not found in store " + nodeSet.size() );
        }
    }

    private static final byte RECORD_NOT_IN_USE = 0;
    private static final byte RECORD_IN_USE = 1;
    private static final int RESERVED = -1;

    private static Set<Integer> checkRelTypeStore( String storeName )
        throws IOException
    {
        File relTypeStore = new File( storeName );
        if ( !relTypeStore.exists() )
        {
            throw new IOException( "Couldn't find relationship type store "
                + storeName );
        }
        File idGenerator = new File( storeName + ".id" );
        if ( idGenerator.exists() )
        {
            boolean success = idGenerator.delete();
            assert success;
        }
        Set<Integer> startBlocks = checkDynamicStore( storeName + ".names" );
        // in_use(byte)+type_blockId(int)
        System.out.print( storeName );
        ByteBuffer buffer = ByteBuffer.allocate( 5 );
        FileChannel fileChannel = new RandomAccessFile( storeName, "r" )
            .getChannel();
        long fileSize = fileChannel.size();
        fileChannel.position( 0 );
        long dot = fileSize / 5 / 20;
        int i = 0;
        int inUseCount = 0;
        Set<Integer> relTypeSet = new java.util.HashSet<Integer>();
        while ( fileChannel.read( buffer ) == 5 )
        {
            buffer.flip();
            byte inUse = buffer.get();
            if ( inUse == RECORD_IN_USE )
            {
                inUseCount++;
                int block = buffer.getInt();
                if ( block != RESERVED && !startBlocks.remove( block ) )
                {
                    throw new IOException( "start block[" + block
                        + "] not found for record " + i );
                }
                relTypeSet.add( i );
            }
            else if ( inUse != RECORD_NOT_IN_USE )
            {
                System.out.println( "Unkown record at: " + i );
                System.out.println( "In use flag was: " + inUse );
            }
            i++;
            if ( dot != 0 && i % dot == 0 )
            {
                System.out.print( "." );
            }
            buffer.clear();
        }
        System.out.print( " high id:" + i + " count:" + inUseCount );
        if ( !startBlocks.isEmpty() )
        {
            System.out.println( "Stray type name blocks found "
                + startBlocks.size() );
        }
        fileChannel.close();
        System.out.println( ".ok" );
        return relTypeSet;
    }

    private static Set<Integer> checkPropertyStore( String storeName,
        Set<Integer> propertyIndex ) throws IOException
    {
        File propStore = new File( storeName );
        if ( !propStore.exists() )
        {
            throw new IOException( "Couldn't find property store " + storeName );
        }
        File idGenerator = new File( storeName + ".id" );
        if ( idGenerator.exists() )
        {
            boolean success = idGenerator.delete();
            assert success;
        }
        Set<Integer> arrayStartBlocks = checkDynamicStore( storeName
            + ".arrays" );
        Set<Integer> stringStartBlocks = checkDynamicStore( storeName
            + ".strings" );
        // in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
        // prev_prop_id(int)+next_prop_id(int)
        int recordSize = 25;
        System.out.print( storeName );
        ByteBuffer buffer = ByteBuffer.allocate( recordSize );
        FileChannel fileChannel = new RandomAccessFile( storeName, "r" )
            .getChannel();
        long fileSize = fileChannel.size();
        fileChannel.position( 0 );
        long dot = fileSize / recordSize / 20;
        Set<Integer> startBlocks = new java.util.HashSet<Integer>();
        int i = 0;
        int inUseCount = 0;
        for ( i = 0; (i + 1) * recordSize <= fileSize; i++ )
        {
            buffer.clear();
            fileChannel.position( (long) i * recordSize );
            fileChannel.read( buffer );
            buffer.flip();
            byte inUse = buffer.get();
            if ( inUse == RECORD_IN_USE )
            {
                inUseCount++;
                int type = buffer.getInt();
                int key = buffer.getInt();
                long prop = buffer.getLong();
                int previous = buffer.getInt();
                int next = buffer.getInt();
                if ( next != NO_NEXT_BLOCK )
                {
                    if ( (next + 1) * recordSize > fileSize || next < 0 )
                    {
                        throw new IOException( "Bad next record[" + next
                            + "] at record " + i );
                    }
                    buffer.clear();
                    fileChannel.position( (long) next * recordSize );
                    fileChannel.read( buffer );
                    buffer.flip();
                    if ( buffer.get() != RECORD_IN_USE )
                    {
                        throw new IOException( "Bad next record[" + next
                            + ",(not in use)] at record " + i );
                    }
                    buffer.getInt();
                    buffer.getInt();
                    buffer.getLong();
                    int prev = buffer.getInt();
                    if ( prev != i )
                    {
                        throw new IOException( "Bad next record[" + next
                            + ",(previous don't match)] at record " + i );
                    }
                }
                if ( previous == NO_PREV_BLOCK )
                {
                    startBlocks.add( i );
                }
                else
                {
                    if ( (previous + 1) * recordSize > fileSize || previous < 0 )
                    {
                        throw new IOException( "Bad previous record["
                            + previous + "] at record " + i );
                    }
                    buffer.clear();
                    fileChannel.position( (long) previous * recordSize );
                    fileChannel.read( buffer );
                    buffer.flip();
                    if ( buffer.get() != RECORD_IN_USE )
                    {
                        throw new IOException( "Bad previous record["
                            + previous + ",(not in use)] at record " + i );
                    }
                    buffer.getInt();
                    buffer.getInt();
                    buffer.getLong();
                    buffer.getInt();
                    int nxt = buffer.getInt();
                    if ( nxt != i )
                    {
                        throw new IOException( "Bad previous record["
                            + previous + ",(next don't match)] at record " + i );
                    }
                }
                if ( type < 1 || type > 10 )
                {
                    throw new IOException( "Bad property type[" + type
                        + "] at record " + i );
                }
                if ( !propertyIndex.contains( key ) )
                {
                    throw new IOException( "key index[" + key
                        + "] not found for record " + i );
                }
                if ( type == 9 && !arrayStartBlocks.remove( (int) prop ) )
                {
                    throw new IOException( "array start block[" + prop
                        + "] not found for record " + i );
                }
                if ( type == 2 && !stringStartBlocks.remove( (int) prop ) )
                {
                    throw new IOException( "string start block[" + prop
                        + "] not found for record " + i );
                }
            }
            else if ( inUse != RECORD_NOT_IN_USE )
            {
                System.out.println( "Unkown record at: " + i );
                System.out.println( "In use flag was: " + inUse );
            }
            if ( dot != 0 && i % dot == 0 )
            {
                System.out.print( "." );
            }
        }
        System.out.print( " high id:" + i + " count:" + inUseCount );
        if ( !stringStartBlocks.isEmpty() )
        {
            System.out.println( "Stray string blocks found "
                + stringStartBlocks.size() );
        }
        fileChannel.close();
        System.out.println( ".ok" );
        return startBlocks;
    }

    private static Set<Integer> checkPropertyIndexStore( String storeName )
        throws IOException
    {
        File indexStore = new File( storeName );
        if ( !indexStore.exists() )
        {
            throw new IOException( "Couldn't find property store " + storeName );
        }
        File idGenerator = new File( storeName + ".id" );
        if ( idGenerator.exists() )
        {
            boolean success = idGenerator.delete();
            assert success;
        }
        Set<Integer> keyStartBlocks = checkDynamicStore( storeName + ".keys" );
        // in_use(byte)+prop_count(int)+key_block_id(int)
        int recordSize = 9;
        System.out.print( storeName );
        ByteBuffer buffer = ByteBuffer.allocate( recordSize );
        FileChannel fileChannel = new RandomAccessFile( storeName, "r" )
            .getChannel();
        long fileSize = fileChannel.size();
        fileChannel.position( 0 );
        long dot = fileSize / recordSize / 20;
        Set<Integer> startBlocks = new java.util.HashSet<Integer>();
        int i = 0;
        int inUseCount = 0;
        for ( i = 0; (i + 1) * recordSize <= fileSize; i++ )
        {
            buffer.clear();
            fileChannel.position( (long) i * recordSize );
            fileChannel.read( buffer );
            buffer.flip();
            byte inUse = buffer.get();
            if ( inUse == RECORD_IN_USE )
            {
                inUseCount++;
                buffer.getInt(); // count
                int key = buffer.getInt();
                if ( !keyStartBlocks.remove( key ) )
                {
                    throw new IOException( "key start block[" + key
                        + "] not found for record " + i );
                }
                startBlocks.add( i );
            }
            else if ( inUse != RECORD_NOT_IN_USE )
            {
                System.out.println( "Unkown record at: " + i );
                System.out.println( "In use flag was: " + inUse );
            }
            if ( dot != 0 && i % dot == 0 )
            {
                System.out.print( "." );
            }
        }
        System.out.print( " high id:" + i + " count:" + inUseCount );
        if ( !keyStartBlocks.isEmpty() )
        {
            System.out.println( "Stray key blocks found "
                + keyStartBlocks.size() );
        }
        fileChannel.close();
        System.out.println( ".ok" );
        return startBlocks;
    }

    private static Set<Integer> checkNodeStore( String storeName,
        Set<Integer> propertySet ) throws IOException
    {
        File nodeStore = new File( storeName );
        if ( !nodeStore.exists() )
        {
            throw new IOException( "Couldn't find node store " + storeName );
        }
        File idGenerator = new File( storeName + ".id" );
        if ( idGenerator.exists() )
        {
            boolean success = idGenerator.delete();
            assert success;
        }
        // in_use(byte)+next_rel_id(int)+next_prop_id(int)
        int recordSize = 9;
        System.out.print( storeName );
        ByteBuffer buffer = ByteBuffer.allocate( recordSize );
        FileChannel fileChannel = new RandomAccessFile( storeName, "r" )
            .getChannel();
        long fileSize = fileChannel.size();
        fileChannel.position( 0 );
        long dot = fileSize / recordSize / 20;
        Set<Integer> nodeSet = new java.util.HashSet<Integer>();
        int i = 0;
        int inUseCount = 0;
        for ( i = 0; (i + 1) * recordSize <= fileSize; i++ )
        {
            buffer.clear();
            fileChannel.position( (long) i * recordSize );
            fileChannel.read( buffer );
            buffer.flip();
            byte inUse = buffer.get();
            if ( inUse == RECORD_IN_USE )
            {
                inUseCount++;
                int nextRel = buffer.getInt();
                int nextProp = buffer.getInt();
                if ( nextRel != NO_NEXT_RELATIONSHIP )
                {
                    nodeSet.add( i );
                }
                if ( nextProp != NO_NEXT_PROPERTY
                    && !propertySet.remove( nextProp ) )
                {
                    throw new IOException( "Bad property start block["
                        + nextProp + "] on record " + i );
                }
            }
            else if ( inUse != RECORD_NOT_IN_USE )
            {
                System.out.println( "Unkown record at: " + i );
                System.out.println( "In use flag was: " + inUse );
            }
            if ( dot != 0 && i % dot == 0 )
            {
                System.out.print( "." );
            }
        }
        System.out.print( " high id:" + i + " count:" + inUseCount );
        fileChannel.close();
        System.out.println( ".ok" );
        return nodeSet;
    }

    private static final byte NOT_DIRECTED = 0;
    private static final byte DIRECTED = 2;
    private static final int NO_NEXT_RELATIONSHIP = -1;
    private static final int NO_PREVIOUS_RELATIONSHIP = -1;
    private static final int NO_NEXT_PROPERTY = -1;

    private static void checkRelationshipStore( String storeName,
        Set<Integer> propertySet, Set<Integer> relTypeSet, Set<Integer> nodeSet )
        throws IOException
    {
        File relStore = new File( storeName );
        if ( !relStore.exists() )
        {
            throw new IOException( "Couldn't find relationship store "
                + storeName );
        }
        File idGenerator = new File( storeName + ".id" );
        if ( idGenerator.exists() )
        {
            boolean success = idGenerator.delete();
            assert success;
        }
        // directed|in_use(byte)+first_node(int)+second_node(int)+rel_type(int)+
        // first_prev_rel(int)+first_next_rel(int)
        // second_prev_rel(int)+second_next_rel(int)+next_prop_id(int)
        int recordSize = 33;
        System.out.print( storeName );
        ByteBuffer buffer = ByteBuffer.allocate( recordSize );
        FileChannel fileChannel = new RandomAccessFile( relStore, "r" )
            .getChannel();
        long fileSize = fileChannel.size();
        fileChannel.position( 0 );
        long dot = fileSize / recordSize / 20;
        int i = 0;
        int inUseCount = 0;
        for ( i = 0; (i + 1) * recordSize <= fileSize; i++ )
        {
            buffer.clear();
            fileChannel.position( (long) i * recordSize );
            fileChannel.read( buffer );
            buffer.flip();
            byte inUse = buffer.get();
            if ( inUse == RECORD_IN_USE + NOT_DIRECTED
                || inUse == RECORD_IN_USE + DIRECTED )
            {
                inUseCount++;
                int firstNode = buffer.getInt();
                int secondNode = buffer.getInt();
                int type = buffer.getInt();
                int firstPrev = buffer.getInt();
                int firstNext = buffer.getInt();
                int secondPrev = buffer.getInt();
                int secondNext = buffer.getInt();
                int prop = buffer.getInt();
                if ( firstPrev == NO_PREVIOUS_RELATIONSHIP
                    && !nodeSet.remove( firstNode ) )
                {
                    throw new IOException( "Bad start node[" + firstNode
                        + "](node don't exist or don't have relationships) "
                        + "at record " + i );
                }
                if ( secondPrev == NO_PREVIOUS_RELATIONSHIP
                    && !nodeSet.remove( secondNode ) )
                {
                    throw new IOException( "Bad start node[" + secondNode
                        + "](node don't exist or don't have relationships) "
                        + "at record " + i );
                }
                checkRelationshipList( i, firstNode, firstPrev, firstNext,
                    fileChannel, buffer );
                checkRelationshipList( i, secondNode, secondPrev, secondNext,
                    fileChannel, buffer );
                if ( prop != NO_NEXT_PROPERTY && !propertySet.remove( prop ) )
                {
                    throw new IOException( "Bad property start block[" + prop
                        + "] on record " + i );
                }
                if ( !relTypeSet.contains( type ) )
                {
                    throw new IOException( "Bad rel type[" + type
                        + "] on record " + i );
                }
            }
            else if ( inUse != RECORD_NOT_IN_USE )
            {
                System.out.println( "Unkown record at: " + i );
                System.out.println( "In use flag was: " + inUse );
            }
            if ( dot != 0 && i % dot == 0 )
            {
                System.out.print( "." );
            }
        }
        System.out.print( " high id:" + i + " count:" + inUseCount );
        fileChannel.close();
        System.out.println( ".ok" );
    }

    private static void checkRelationshipList( int i, int node, int prev,
        int next, FileChannel fileChannel, ByteBuffer buffer )
        throws IOException
    {
        long fileSize = fileChannel.size();
        int recordSize = 33;
        if ( next != NO_NEXT_BLOCK )
        {
            if ( (next + 1) * recordSize > fileSize || next < 0 )
            {
                throw new IOException( "Bad next record[" + next
                    + "] at record " + i );
            }
            buffer.clear();
            fileChannel.position( (long) next * recordSize );
            fileChannel.read( buffer );
            buffer.flip();
            byte inUse = buffer.get();
            if ( inUse != RECORD_IN_USE + NOT_DIRECTED
                && inUse != RECORD_IN_USE + DIRECTED )
            {
                throw new IOException( "Bad next record[" + next
                    + ",(not in use)] at record " + i );
            }
            int firstNode = buffer.getInt();
            int secondNode = buffer.getInt();
            buffer.getInt();
            int firstPrev = buffer.getInt();
            buffer.getInt(); // firstNext
            int secondPrev = buffer.getInt();
            buffer.getInt(); // secondNext
            if ( firstNode != node && secondNode != node )
            {
                throw new IOException( "Bad next record[" + next
                    + "],(nodes don't match)] at record " + i );
            }
            if ( firstNode == node && firstPrev != i )
            {
                throw new IOException( "Bad next record[" + next
                    + ",(previous don't match)] at record " + i );
            }
            if ( secondNode == node && secondPrev != i )
            {
                throw new IOException( "Bad next record[" + next
                    + ",(previous don't match)] at record " + i );
            }
        }
        if ( prev != NO_PREV_BLOCK )
        {
            if ( (prev + 1) * recordSize > fileSize || prev < 0 )
            {
                throw new IOException( "Bad previous record[" + prev
                    + "] at record " + i );
            }
            buffer.clear();
            fileChannel.position( (long) prev * recordSize );
            fileChannel.read( buffer );
            buffer.flip();
            byte inUse = buffer.get();
            if ( inUse != RECORD_IN_USE + NOT_DIRECTED
                && inUse != RECORD_IN_USE + DIRECTED )
            {
                throw new IOException( "Bad previous record[" + prev
                    + ",(not in use)] at record " + i );
            }
            int firstNode = buffer.getInt();
            int secondNode = buffer.getInt();
            buffer.getInt();
            buffer.getInt(); // firstPrev
            int firstNext = buffer.getInt();
            buffer.getInt(); // secondPrev
            int secondNext = buffer.getInt();
            if ( firstNode != node && secondNode != node )
            {
                throw new IOException( "Bad next record[" + next
                    + "],(nodes don't match)] at record " + i );
            }
            if ( firstNode == node && firstNext != i )
            {
                throw new IOException( "Bad previous record[" + prev
                    + ",(next don't match)] at record " + i );
            }
            if ( secondNode == node && secondNext != i )
            {
                throw new IOException( "Bad previous record[" + prev
                    + ",(next don't match)] at record " + i );
            }
        }
    }

    // in_use(byte)+prev_block(int)+nr_of_bytes(int)+next_block(int)
    private static final int BLOCK_HEADER_SIZE = 1 + 4 + 4 + 4;
    private static final int NO_NEXT_BLOCK = -1;
    private static final int NO_PREV_BLOCK = -1;
    private static final byte BLOCK_IN_USE = (byte) 1;
    private static final byte BLOCK_NOT_IN_USE = (byte) 0;

    private static Set<Integer> checkDynamicStore( String storeName )
        throws IOException
    {
        File dynamicStore = new File( storeName );
        if ( !dynamicStore.exists() )
        {
            throw new IOException( "Couldn't find dynamic store " + storeName );
        }
        File idGenerator = new File( storeName + ".blockid" );
        if ( idGenerator.exists() )
        {
            boolean success = idGenerator.delete();
            assert success;
        }
        System.out.print( storeName );
        FileChannel fileChannel = new RandomAccessFile( storeName, "r" )
            .getChannel();
        ByteBuffer buffer = ByteBuffer.allocate( 4 );
        fileChannel.position( 0 );
        if ( fileChannel.read( buffer ) != 4 )
        {
            throw new IOException( "Unable to read blocksize" );
        }
        buffer.flip();
        int blockSize = buffer.getInt();
        int dataSize = blockSize - BLOCK_HEADER_SIZE;
        long fileSize = fileChannel.size();
        buffer = ByteBuffer.allocate( BLOCK_HEADER_SIZE );
        ByteBuffer inUseBuffer = ByteBuffer.allocate( 1 );
        long dot = fileSize / blockSize / 20;
        Set<Integer> startBlocks = new java.util.HashSet<Integer>();
        int i = 0;
        int inUseCount = 0;
        for ( i = 1; (i + 1) * blockSize <= fileSize; i++ )
        {
            inUseBuffer.clear();
            fileChannel.position( (long) i * blockSize );
            fileChannel.read( inUseBuffer );
            inUseBuffer.flip();
            byte inUse = inUseBuffer.get();
            if ( inUse == BLOCK_IN_USE )
            {
                inUseCount++;
                buffer.clear();
                fileChannel.read( buffer );
                buffer.flip();
                int previous = buffer.getInt();
                int bytes = buffer.getInt();
                int next = buffer.getInt();
                if ( next != NO_NEXT_BLOCK && bytes != dataSize
                    || bytes > dataSize || bytes < 0 )
                {
                    throw new IOException( "Bad data size[" + bytes
                        + "] at block " + i + " in " + storeName );
                }
                else if ( next != NO_NEXT_BLOCK )
                {
                    if ( (next + 1) * blockSize > fileSize || next < 0 )
                    {
                        throw new IOException( "Bad next block[" + next
                            + "] at block " + i );
                    }
                    buffer.clear();
                    fileChannel.position( (long) next * blockSize );
                    fileChannel.read( buffer );
                    buffer.flip();
                    if ( buffer.get() != BLOCK_IN_USE )
                    {
                        throw new IOException( "Bad next block[" + next
                            + ",(not in use)] at block " + i );
                    }
                    int prev = buffer.getInt();
                    if ( prev != i )
                    {
                        throw new IOException( "Bad next block[" + next
                            + ",(previous don't match)] at block " + i );
                    }
                }
                if ( previous == NO_PREV_BLOCK )
                {
                    startBlocks.add( i );
                }
                else
                {
                    if ( (previous + 1) * blockSize > fileSize || previous < 0 )
                    {
                        throw new IOException( "Bad previous block[" + previous
                            + "] at block " + i );
                    }
                    buffer.clear();
                    fileChannel.position( (long) previous * blockSize );
                    fileChannel.read( buffer );
                    buffer.flip();
                    if ( buffer.get() != BLOCK_IN_USE )
                    {
                        throw new IOException( "Bad previous block[" + previous
                            + ",(not in use)] at block " + i );
                    }
                    buffer.getInt();
                    buffer.getInt();
                    int nxt = buffer.getInt();
                    if ( nxt != i )
                    {
                        throw new IOException( "Bad previous block[" + previous
                            + ",(next don't match)] at block " + i );
                    }
                }
            }
            else if ( inUse != BLOCK_NOT_IN_USE )
            {
                System.out.println( "Unkown record at: " + i );
                System.out.println( "In use flag was: " + inUse );
            }
            if ( dot != 0 && i % dot == 0 )
            {
                System.out.print( "." );
            }
        }
        System.out.print( " high id:" + i + " count:" + inUseCount );
        fileChannel.close();
        System.out.println( ".ok" );
        return startBlocks;
    }
}
