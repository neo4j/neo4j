/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.command;

import org.junit.Test;

import java.util.function.Supplier;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingUpdateService;
import org.neo4j.kernel.impl.api.index.PropertyPhysicalToLogicalConverter;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.util.concurrent.WorkSync;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

public class IndexBatchTransactionApplierTest
{
    @Test
    public void shouldProvideLabelScanStoreUpdatesSortedByNodeId() throws Exception
    {
        // GIVEN
        IndexingService indexing = mock( IndexingService.class );
        when( indexing.convertToIndexUpdates( any(), eq( EntityType.NODE ) ) ).thenAnswer( o -> Iterables.empty() );
        LabelScanWriter writer = new OrderVerifyingLabelScanWriter( 10, 15, 20 );
        WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork> labelScanSync =
                spy( new WorkSync<>( singletonProvider( writer ) ) );
        WorkSync<IndexingUpdateService,IndexUpdatesWork> indexUpdatesSync = new WorkSync<>( indexing );
        TransactionToApply tx = mock( TransactionToApply.class );
        PropertyStore propertyStore = mock( PropertyStore.class );
        try ( IndexBatchTransactionApplier applier = new IndexBatchTransactionApplier( indexing, labelScanSync, indexUpdatesSync, mock( NodeStore.class ),
                mock( RelationshipStore.class ), new PropertyPhysicalToLogicalConverter( propertyStore ) ) )
        {
            try ( TransactionApplier txApplier = applier.startTx( tx ) )
            {
                // WHEN
                txApplier.visitNodeCommand( node( 15 ) );
                txApplier.visitNodeCommand( node( 20 ) );
                txApplier.visitNodeCommand( node( 10 ) );
            }
        }
        // THEN all assertions happen inside the LabelScanWriter#write and #close
        verify( labelScanSync ).applyAsync( any() );
    }

    private Supplier<LabelScanWriter> singletonProvider( final LabelScanWriter writer )
    {
        return () -> writer;
    }

    private NodeCommand node( long nodeId )
    {
        NodeRecord after = new NodeRecord( nodeId,
                true, false, NO_NEXT_RELATIONSHIP.intValue(),NO_NEXT_PROPERTY.intValue(), 0 );
        NodeLabelsField.parseLabelsField( after ).add( 1, null, null );

        return new NodeCommand( new NodeRecord( nodeId ), after );
    }

    private static class OrderVerifyingLabelScanWriter implements LabelScanWriter
    {
        private final long[] expectedNodeIds;
        private int cursor;

        OrderVerifyingLabelScanWriter( long... expectedNodeIds )
        {
            this.expectedNodeIds = expectedNodeIds;
        }

        @Override
        public void write( NodeLabelUpdate update )
        {
            assertEquals( expectedNodeIds[cursor], update.getNodeId() );
            cursor++;
        }

        @Override
        public void close()
        {
            assertEquals( cursor, expectedNodeIds.length );
        }
    }
}
