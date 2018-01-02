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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.logging.LogProvider;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_READ_AHEAD;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.kernel.impl.store.AbstractDynamicStore.readFullByteArrayFromHeavyRecords;

/**
 * Implementation of the node store.
 */
public class NodeStore extends AbstractRecordStore<NodeRecord>
{
    public static Long readOwnerFromDynamicLabelsRecord( DynamicRecord record )
    {
        byte[] data = record.getData();
        byte[] header = PropertyType.ARRAY.readDynamicRecordHeader( data );
        byte[] array = Arrays.copyOfRange( data, header.length, data.length );

        int requiredBits = header[2];
        if ( requiredBits == 0 )
        {
            return null;
        }
        Bits bits = Bits.bitsFromBytes( array );
        return bits.getLong( requiredBits );
    }

    public static abstract class Configuration
        extends AbstractStore.Configuration
    {
    }

    public static final String TYPE_DESCRIPTOR = "NodeStore";

    // in_use(byte)+next_rel_id(int)+next_prop_id(int)+labels(5)+extra(byte)
    public static final int RECORD_SIZE = 15;

    private final DynamicArrayStore dynamicLabelStore;

    public NodeStore(
            File fileName,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            DynamicArrayStore dynamicLabelStore )
    {
        super( fileName, config, IdType.NODE, idGeneratorFactory, pageCache, logProvider );
        this.dynamicLabelStore = dynamicLabelStore;
    }

    @Override
    public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, NodeRecord record ) throws FAILURE
    {
        processor.processNode( this, record );
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
        return getRecordSize();
    }

    public void ensureHeavy( NodeRecord node )
    {
        if ( NodeLabelsField.fieldPointsToDynamicRecordOfLabels( node.getLabelField() ) )
        {
            ensureHeavy( node, NodeLabelsField.firstDynamicLabelRecordId( node.getLabelField() ) );
        }
    }

    public void ensureHeavy( NodeRecord node, long firstDynamicLabelRecord )
    {
        if ( !node.isLight() )
        {
            return;
        }

        // Load any dynamic labels and populate the node record
        node.setLabelField( node.getLabelField(), dynamicLabelStore.getRecords( firstDynamicLabelRecord ) );
    }

    @Override
    public NodeRecord getRecord( long id )
    {
        return getRecord( id, null );
    }

    public NodeRecord getRecord( long id, NodeRecord record )
    {
        NodeRecord result = loadRecord( id, record );

        if ( result == null )
        {
            throw new InvalidRecordException( "NodeRecord[" + id + "] not in use" );
        }
        return result;
    }

    public NodeRecord loadLightNode( long id )
    {
        return loadRecord( id, null );
    }

    @Override
    public NodeRecord forceGetRecord( long id )
    {
        NodeRecord record = loadRecord( id, null );
        if ( record == null )
        {
            return new NodeRecord(
                    id,
                    false,
                    Record.NO_NEXT_RELATIONSHIP.intValue(),
                    Record.NO_NEXT_PROPERTY.intValue() ); // inUse=false by default
        }
        return record;
    }

    public NodeRecord loadRecord( long id, NodeRecord record)
    {
        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );
        try ( PageCursor cursor = storeFile.io( pageId, PF_SHARED_LOCK ) )
        {
            boolean isInUse = false;
            if ( cursor.next() )
            {
                do
                {
                    cursor.setOffset( offset );

                    // [    ,   x] in use bit
                    // [    ,xxx ] higher bits for rel id
                    // [xxxx,    ] higher bits for prop id
                    byte inUseByte = cursor.getByte();
                    isInUse = isInUse( inUseByte );
                    if ( isInUse )
                    {
                        if ( record == null )
                        {
                            record = new NodeRecord( id );
                        }
                        record.setId( id );
                        readIntoRecord( cursor, record, inUseByte, true );
                    }
                } while ( cursor.shouldRetry() );
            }
            if ( isInUse )
            {
                return record;
            }
            if ( record != null )
            {
                record.setInUse( false );
            }
            return null;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public void forceUpdateRecord( NodeRecord record )
    {
        writeRecord( record, true );
    }

    @Override
    public void updateRecord( NodeRecord record )
    {
        writeRecord( record, false );
        if ( !record.inUse() )
        {
            freeId( record.getId() );
        }
        updateDynamicLabelRecords( record.getDynamicLabelRecords() );
    }

    private void writeRecord( NodeRecord record, boolean force )
    {
        long recordId = record.getId();
        long pageId = pageIdForRecord( recordId );
        try ( PageCursor cursor = storeFile.io( pageId, PF_EXCLUSIVE_LOCK ) )
        {
            if ( cursor.next() )
            {
                do
                {
                    writeRecord( cursor, record, force );
                } while ( cursor.shouldRetry() );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private void writeRecord( PageCursor cursor, NodeRecord record, boolean force )
    {
        int offset = offsetForId( record.getId() );
        cursor.setOffset( offset );
        if ( record.inUse() || force )
        {
            long nextRel = record.getNextRel();
            long nextProp = record.getNextProp();

            short relModifier = nextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (short)((nextRel & 0x700000000L) >> 31);
            short propModifier = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (short)((nextProp & 0xF00000000L) >> 28);

            // [    ,   x] in use bit
            // [    ,xxx ] higher bits for rel id
            // [xxxx,    ] higher bits for prop id
            short inUseUnsignedByte = ( record.inUse() ? Record.IN_USE : Record.NOT_IN_USE ).byteValue();
            inUseUnsignedByte = (short) ( inUseUnsignedByte | relModifier | propModifier );

            cursor.putByte( (byte) inUseUnsignedByte );
            cursor.putInt( (int) nextRel );
            cursor.putInt( (int) nextProp );

            // lsb of labels
            long labelField = record.getLabelField();
            cursor.putInt( (int) labelField );
            // msb of labels
            cursor.putByte( (byte) ((labelField & 0xFF00000000L) >> 32) );

            byte extra = record.isDense() ? (byte)1 : (byte)0;
            cursor.putByte( extra );
        }
        else
        {
            cursor.putByte( Record.NOT_IN_USE.byteValue() );
        }
    }

    public boolean inUse( long id )
    {
        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );

        try ( PageCursor cursor = storeFile.io( pageId, PF_SHARED_LOCK ) )
        {
            boolean recordIsInUse = false;
            if ( cursor.next() )
            {
                do
                {
                    cursor.setOffset( offset );
                    recordIsInUse = isInUse( cursor.getByte() );
                } while ( cursor.shouldRetry() );
            }
            return recordIsInUse;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private void readIntoRecord( PageCursor cursor, NodeRecord record, byte inUseByte, boolean inUse )
    {
        long nextRel = cursor.getUnsignedInt();
        long nextProp = cursor.getUnsignedInt();

        long relModifier = (inUseByte & 0xEL) << 31;
        long propModifier = (inUseByte & 0xF0L) << 28;

        long lsbLabels = cursor.getUnsignedInt();
        long hsbLabels = cursor.getByte() & 0xFF; // so that a negative byte won't fill the "extended" bits with ones.
        long labels = lsbLabels | (hsbLabels << 32);
        byte extra = cursor.getByte();
        boolean dense = (extra & 0x1) > 0;

        record.setDense( dense );
        record.setNextRel( longFromIntAndMod( nextRel, relModifier ) );
        record.setNextProp( longFromIntAndMod( nextProp, propModifier ) );
        record.setInUse( inUse );
        record.setLabelField( labels, Collections.<DynamicRecord>emptyList() );
    }

    /**
     * Scan the given range of records both inclusive, and pass all the in-use ones to the given processor, one by one.
     *
     * The record passed to the NodeRecordScanner is reused instead of reallocated for every record, so it must be
     * cloned if you want to save it for later.
     */
    public void scanAllRecords( Visitor<NodeRecord,IOException> visitor ) throws IOException
    {
        long startPageId = pageIdForRecord( 0 );
        long currentPageId = startPageId;
        long endPageId = pageIdForRecord( getHighestPossibleIdInUse() );
        long currentRecordId = 0;
        NodeRecord record = new NodeRecord( -1 );
        int recordsPerPage = storeFile.pageSize() / getRecordSize();

        try ( PageCursor cursor = storeFile.io( startPageId, PF_SHARED_LOCK | PF_READ_AHEAD ) )
        {
            while ( currentPageId <= endPageId && cursor.next() )
            {
                for ( int i = 0; i < recordsPerPage; i++ )
                {
                    record.setId( currentRecordId );
                    int offset = offsetForId( currentRecordId );
                    do {
                        cursor.setOffset( offset );
                        byte inUseByte = cursor.getByte();
                        boolean isInUse = isInUse( inUseByte );
                        readIntoRecord( cursor, record, inUseByte, isInUse );
                    } while ( cursor.shouldRetry() );

                    if ( record.inUse() )
                    {
                        if ( visitor.visit( record ) )
                        {
                            return;
                        }
                    }
                    currentRecordId++;
                }
                currentPageId++;
            }
        }
    }

    public DynamicArrayStore getDynamicLabelStore()
    {
        return dynamicLabelStore;
    }

    public Collection<DynamicRecord> allocateRecordsForDynamicLabels( long nodeId, long[] labels,
            Iterator<DynamicRecord> useFirst )
    {
        return allocateRecordsForDynamicLabels( nodeId, labels, useFirst, dynamicLabelStore );
    }

    public static Collection<DynamicRecord> allocateRecordsForDynamicLabels( long nodeId, long[] labels,
            Iterator<DynamicRecord> useFirst, DynamicRecordAllocator allocator )
    {
        long[] storedLongs = LabelIdArray.prependNodeId( nodeId, labels );
        Collection<DynamicRecord> records = new ArrayList<>();
        DynamicArrayStore.allocateRecords( records, storedLongs, useFirst, allocator );
        return records;
    }

    public long[] getDynamicLabelsArray( Iterable<DynamicRecord> records )
    {
        long[] storedLongs = (long[])
            DynamicArrayStore.getRightArray( dynamicLabelStore.readFullByteArray( records, PropertyType.ARRAY ) );
        return LabelIdArray.stripNodeId( storedLongs );
    }

    public static long[] getDynamicLabelsArrayFromHeavyRecords( Iterable<DynamicRecord> records )
    {
        long[] storedLongs = (long[])
            DynamicArrayStore.getRightArray( readFullByteArrayFromHeavyRecords( records, PropertyType.ARRAY ) );
        return LabelIdArray.stripNodeId( storedLongs );
    }

    public Pair<Long, long[]> getDynamicLabelsArrayAndOwner( Iterable<DynamicRecord> records )
    {
        long[] storedLongs = (long[])
                DynamicArrayStore.getRightArray( dynamicLabelStore.readFullByteArray( records, PropertyType.ARRAY ) );
        return Pair.of(storedLongs[0], LabelIdArray.stripNodeId( storedLongs ));
    }

    public void updateDynamicLabelRecords( Iterable<DynamicRecord> dynamicLabelRecords )
    {
        for ( DynamicRecord record : dynamicLabelRecords )
        {
            dynamicLabelStore.updateRecord( record );
        }
    }
}
