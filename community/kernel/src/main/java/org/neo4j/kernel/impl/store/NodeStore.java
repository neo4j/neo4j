/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.nio.file.OpenOption;
import java.util.Arrays;
import java.util.List;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.StoreStatement;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.store.NoStoreHeaderFormat.NO_STORE_HEADER_FORMAT;

/**
 * Implementation of the node store.
 */
public class NodeStore extends CommonAbstractStore<NodeRecord,NoStoreHeader> implements StoreStatement.Nodes
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

    @Override
    public RecordCursor<DynamicRecord> newLabelCursor()
    {
        return dynamicLabelStore.newRecordCursor( dynamicLabelStore.newRecord() ).acquire( getNumberOfReservedLowIds(),
                RecordLoad.NORMAL );
    }

    public abstract static class Configuration
        extends CommonAbstractStore.Configuration
    {
    }

    public static final String TYPE_DESCRIPTOR = "NodeStore";

    private final DynamicArrayStore dynamicLabelStore;

    public NodeStore(
            File fileName,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            DynamicArrayStore dynamicLabelStore,
            RecordFormats recordFormats,
            OpenOption... openOptions )
    {
        super( fileName, config, IdType.NODE, idGeneratorFactory, pageCache, logProvider, TYPE_DESCRIPTOR,
                recordFormats.node(), NO_STORE_HEADER_FORMAT, recordFormats.storeVersion(), openOptions );
        this.dynamicLabelStore = dynamicLabelStore;
    }

    @Override
    public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, NodeRecord record ) throws FAILURE
    {
        processor.processNode( this, record );
    }

    @Override
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
        node.setLabelField( node.getLabelField(), dynamicLabelStore.getRecords( firstDynamicLabelRecord, RecordLoad.NORMAL ) );
    }

    public static void ensureHeavy( NodeRecord node, RecordCursor<DynamicRecord> dynamicLabelCursor )
    {
        long firstDynamicLabelId = NodeLabelsField.firstDynamicLabelRecordId( node.getLabelField() );
        dynamicLabelCursor.placeAt( firstDynamicLabelId, RecordLoad.NORMAL );
        List<DynamicRecord> dynamicLabelRecords = dynamicLabelCursor.getAll();
        node.setLabelField( node.getLabelField(), dynamicLabelRecords );
    }

    @Override
    public void updateRecord( NodeRecord record )
    {
        super.updateRecord( record );
        updateDynamicLabelRecords( record.getDynamicLabelRecords() );
    }

    public DynamicArrayStore getDynamicLabelStore()
    {
        return dynamicLabelStore;
    }

    public void updateDynamicLabelRecords( Iterable<DynamicRecord> dynamicLabelRecords )
    {
        for ( DynamicRecord record : dynamicLabelRecords )
        {
            dynamicLabelStore.updateRecord( record );
        }
    }
}
