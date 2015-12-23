/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.command;

import org.junit.Test;

import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.concurrent.WorkSync;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

public class IndexBatchTransactionApplierTest
{
    @Test
    public void shouldProvideLabelScanStoreUpdatesSortedByNodeId() throws Exception
    {
        // GIVEN
        IndexingService indexing = mock( IndexingService.class );
        LabelScanWriter writer = new OrderVerifyingLabelScanWriter( 10, 15, 20 );
        WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork> labelScanSync =
                spy( new WorkSync<>( singletonProvider( writer ) ) );
        WorkSync<IndexingService,IndexUpdatesWork> indexUpdatesSync = new WorkSync<>( indexing );
        TransactionToApply tx = mock( TransactionToApply.class );
        try ( IndexBatchTransactionApplier applier = new IndexBatchTransactionApplier( indexing, labelScanSync,
                indexUpdatesSync, mock( NodeStore.class ), mock( PropertyStore.class ),
                mock( PropertyLoader.class ), TransactionApplicationMode.INTERNAL ) )
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
        verify( labelScanSync ).apply( any() );
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
        public void write( NodeLabelUpdate update ) throws IOException
        {
            assertEquals( expectedNodeIds[cursor], update.getNodeId() );
            cursor++;
        }

        @Override
        public void close() throws IOException
        {
            assertEquals( cursor, expectedNodeIds.length );
        }
    }
}
