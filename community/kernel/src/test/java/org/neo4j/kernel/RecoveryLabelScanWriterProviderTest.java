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
package org.neo4j.kernel;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.Provider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RecoveryLabelScanWriterProviderTest
{
    @Test
    public void shouldOnlyWriteChangesAfterFullBatch() throws Exception
    {
        // GIVEN
        LabelScanStore store = mock( LabelScanStore.class );
        LabelScanWriter storeWriter = mock( LabelScanWriter.class );
        when( store.newWriter() ).thenReturn( storeWriter );
        int batchSize = 5;

        // WHEN
        List<NodeLabelUpdate> expectedUpdates = new ArrayList<>();
        try ( RecoveryLabelScanWriterProvider provider = new RecoveryLabelScanWriterProvider( store, batchSize ) )
        {
            for ( int b = 0; b < 3; b++ )
            {
                // Simulate a couple of transactions, with node updates not sorted on node id on purpose
                for ( int i = 0; i < batchSize-1; i++ )
                {
                    simulateTransaction( provider, b*100 + batchSize-i, expectedUpdates );
                    verifyNoMoreInteractions( storeWriter );
                }
                simulateTransaction( provider, b*100 + batchSize*2, expectedUpdates );

                // At this point they should've arrived
                for ( NodeLabelUpdate update : expectedUpdates )
                {
                    verify( storeWriter ).write( update );
                }
                verify( storeWriter ).close();
                verifyNoMoreInteractions( storeWriter );
                reset( storeWriter );
                expectedUpdates.clear();
            }
        }
    }

    private void simulateTransaction( Provider<LabelScanWriter> provider, int i, List<NodeLabelUpdate> expectedUpdates )
            throws Exception
    {
        try ( LabelScanWriter writer = provider.instance() )
        {
            NodeLabelUpdate update = NodeLabelUpdate.labelChanges( i, longs(), longs( 1 ) );
            expectedUpdates.add( update );
            writer.write( update );
        }
    }

    private long[] longs( long... longs )
    {
        return longs;
    }
}
