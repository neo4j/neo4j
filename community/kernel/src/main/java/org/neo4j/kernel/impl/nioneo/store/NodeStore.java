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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.xa.NodeLabelRecordLogic;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Implementation of the node store.
 */
public class NodeStore extends AbstractStore implements Store, RecordStore<NodeRecord>
{
    public static abstract class Configuration
        extends AbstractStore.Configuration
    {
    }

    public static final String TYPE_DESCRIPTOR = "NodeStore";

    // in_use(byte)+next_rel_id(int)+next_prop_id(int)+labels(5)
    public static final int RECORD_SIZE = 14;

    private DynamicArrayStore dynamicLabelStore;

    public NodeStore(File fileName, Config config,
                     IdGeneratorFactory idGeneratorFactory, WindowPoolFactory windowPoolFactory,
                     FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger,
                     DynamicArrayStore dynamicLabelStore )
    {
        super(fileName, config, IdType.NODE, idGeneratorFactory, windowPoolFactory, fileSystemAbstraction, stringLogger);
        this.dynamicLabelStore = dynamicLabelStore;
    }

    @Override
    public void accept( RecordStore.Processor processor, NodeRecord record )
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
    
    public void makeHeavy( NodeRecord node )
    {
        long labels = node.getLabelField();
        byte header = NodeLabelRecordLogic.getHeader( labels );
        if ( NodeLabelRecordLogic.highHeaderBitSet( header ) )
        {
            long firstDynamicRecord = NodeLabelRecordLogic.parseLabelsBody( labels );
            makeHeavy( node, firstDynamicRecord );
        }
    }
    
    public void makeHeavy( NodeRecord node, long firstDynamicLabelRecord )
    {
        if ( !node.isLight() )
            return;
        
        // Load any dynamic labels and populate the node record
        node.setLabelField( node.getLabelField(), dynamicLabelStore.getRecords( firstDynamicLabelRecord ) );
    }

    @Override
    public NodeRecord getRecord( long id )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            return getRecord( id, window, RecordLoad.NORMAL );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    @Override
    public NodeRecord forceGetRecord( long id )
    {
        PersistenceWindow window = null;
        try
        {
            window = acquireWindow( id, OperationType.READ );
        }
        catch ( InvalidRecordException e )
        {
            return new NodeRecord( id, Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_NEXT_PROPERTY.intValue() ); // inUse=false by default
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

    @Override
    public NodeRecord forceGetRaw( NodeRecord record )
    {
        return record;
    }

    @Override
    public NodeRecord forceGetRaw( long id )
    {
        return forceGetRecord( id );
    }

//    public void updateRecord( NodeRecord record, boolean recovered )
//    {
//        assert recovered;
//        setRecovered();
//        try
//        {
//            updateRecord( record );
//            registerIdFromUpdateRecord( record.getId() );
//        }
//        finally
//        {
//            unsetRecovered();
//        }
//    }

    @Override
    public void forceUpdateRecord( NodeRecord record )
    {
        PersistenceWindow window = acquireWindow( record.getId(),
                OperationType.WRITE );
            try
            {
                updateRecord( record, window, true );
            }
            finally
            {
                releaseWindow( window );
            }
    }

    @Override
    public void updateRecord( NodeRecord record )
    {
        PersistenceWindow window = acquireWindow( record.getId(),
            OperationType.WRITE );
        try
        {
            updateRecord( record, window, false );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    public NodeRecord loadLightNode( long id )
    {
        PersistenceWindow window = null;
        try
        {
            window = acquireWindow( id, OperationType.READ );
        }
        catch ( InvalidRecordException e )
        {
            // ok id to high
            return null;
        }

        try
        {
            return getRecord( id, window, RecordLoad.CHECK );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    private NodeRecord getRecord( long id, PersistenceWindow window,
        RecordLoad load  )
    {
        Buffer buffer = window.getOffsettedBuffer( id );

        // [    ,   x] in use bit
        // [    ,xxx ] higher bits for rel id
        // [xxxx,    ] higher bits for prop id
        long inUseByte = buffer.get();

        boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
        if ( !inUse )
        {
            switch ( load )
            {
            case NORMAL:
                throw new InvalidRecordException( "NodeRecord[" + id + "] not in use" );
            case CHECK:
                return null;
            case FORCE:
                break;
            }
        }

        long nextRel = buffer.getUnsignedInt();
        long nextProp = buffer.getUnsignedInt();

        long relModifier = (inUseByte & 0xEL) << 31;
        long propModifier = (inUseByte & 0xF0L) << 28;
        
        long lsbLabels = buffer.getUnsignedInt();
        long hsbLabels = buffer.get();
        long labels = lsbLabels | (hsbLabels << 32);

        NodeRecord nodeRecord = new NodeRecord( id, longFromIntAndMod( nextRel, relModifier ), longFromIntAndMod( nextProp, propModifier ) );
        nodeRecord.setInUse( inUse );
        nodeRecord.setLabelField( labels );
        
        return nodeRecord;
    }

    private void updateRecord( NodeRecord record, PersistenceWindow window, boolean force )
    {
        long id = record.getId();
        registerIdFromUpdateRecord( id );
        Buffer buffer = window.getOffsettedBuffer( id );
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
            buffer.put( (byte) inUseUnsignedByte ).putInt( (int) nextRel ).putInt( (int) nextProp );
            
            // lsb of labels
            long labelField = record.getLabelField();
            buffer.putInt( (int) labelField );
            // msb of labels
            buffer.put( (byte) ((labelField&0xFF00000000L) >>> 32) );
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

    @Override
    public List<WindowPoolStats> getAllWindowPoolStats()
    {
        List<WindowPoolStats> list = new ArrayList<WindowPoolStats>();
        list.add( getWindowPoolStats() );
        return list;
    }
    
    public DynamicArrayStore getDynamicLabelStore()
    {
        return dynamicLabelStore;
    }
    
    @Override
    protected void closeStorage()
    {
        if ( dynamicLabelStore != null )
        {
            dynamicLabelStore.close();
            dynamicLabelStore = null;
        }
    }
    
    public Collection<DynamicRecord> allocateRecordsForDynamicLabels( long[] labels )
    {
        return allocateRecordsForDynamicLabels( labels, Collections.<DynamicRecord>emptyList().iterator() );
    }
    
    public Collection<DynamicRecord> allocateRecordsForDynamicLabels( long[] labels, Iterator<DynamicRecord> useFirst )
    {
        return dynamicLabelStore.allocateRecords( labels, useFirst );
    }

    public long[] getDynamicLabelsArray( Iterable<DynamicRecord> records )
    {
        return (long[]) dynamicLabelStore.getRightArray( dynamicLabelStore.readFullByteArray(
                records, PropertyType.ARRAY ) );
    }
    
    public void updateDynamicLabelRecords( Iterable<DynamicRecord> dynamicLabelRecords )
    {
        for ( DynamicRecord record : dynamicLabelRecords )
            dynamicLabelStore.updateRecord( record );
    }
    
    public long[] getLabelsForNode( NodeRecord node )
    {
        long labels = node.getLabelField();
        byte header = NodeLabelRecordLogic.getHeader( labels );
        if ( !NodeLabelRecordLogic.highHeaderBitSet( header ) )
            return NodeLabelRecordLogic.parseInlined( labels, header );
        else
        {
            long firstDynamicRecord = NodeLabelRecordLogic.parseLabelsBody( labels );
            makeHeavy( node, firstDynamicRecord );
            return getDynamicLabelsArray( node.getDynamicLabelRecords() );
        }
    }
    
    @Override
    protected void setRecovered()
    {
        dynamicLabelStore.setRecovered();
        super.setRecovered();
    }
    
    @Override
    protected void unsetRecovered()
    {
        dynamicLabelStore.unsetRecovered();
        super.unsetRecovered();
    }
    
    @Override
    public void makeStoreOk()
    {
        dynamicLabelStore.makeStoreOk();
        super.makeStoreOk();
    }
    
    @Override
    public void rebuildIdGenerators()
    {
        dynamicLabelStore.rebuildIdGenerators();
        super.rebuildIdGenerators();
    }
    
    protected void updateIdGenerators()
    {
        dynamicLabelStore.updateHighId();
        super.updateHighId();
    }
    
    @Override
    public void flushAll()
    {
        dynamicLabelStore.flushAll();
        super.flushAll();
    }
}
