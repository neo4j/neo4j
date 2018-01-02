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
package org.neo4j.kernel.impl.transaction.command;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.concurrent.WorkSync;
import org.neo4j.helpers.Provider;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import static org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates.NONE;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

public class IndexTransactionApplierTest
{
    @Test
    public void shouldProvideLabelScanStoreUpdatesSortedByNodeId() throws Exception
    {
        // GIVEN
        IndexingService indexing = mock( IndexingService.class );
        LabelScanWriter writer = new OrderVerifyingLabelScanWriter( 10, 15, 20 );
        WorkSync<Provider<LabelScanWriter>,IndexTransactionApplier.LabelUpdateWork> labelScanSync =
                new WorkSync<>( singletonProvider( writer ) );
        try ( IndexTransactionApplier applier = new IndexTransactionApplier( indexing, NONE, labelScanSync ) )
        {
            // WHEN
            applier.visitNodeCommand( node( 15 ) );
            applier.visitNodeCommand( node( 20 ) );
            applier.visitNodeCommand( node( 10 ) );
            applier.apply();
        }
        // THEN all assertions happen inside the LabelScanWriter#write and #close
    }

    private Provider<LabelScanWriter> singletonProvider( final LabelScanWriter writer )
    {
        return new Provider<LabelScanWriter>()
        {
            @Override
            public LabelScanWriter instance()
            {
                return writer;
            }
        };
    }

    private NodeCommand node( long nodeId )
    {
        NodeCommand command = new NodeCommand();
        NodeRecord after = new NodeRecord( nodeId,
                true, false, NO_NEXT_RELATIONSHIP.intValue(),NO_NEXT_PROPERTY.intValue(), 0 );
        NodeLabelsField.parseLabelsField( after ).add( 1, null, null );

        command.init( new NodeRecord( nodeId ), after );
        return command;
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
        public void write( NodeLabelUpdate update ) throws IOException, IndexCapacityExceededException
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
