/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.consistency.checking.full;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Test;

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.direct.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.direct.NodeLabelRange;
import org.neo4j.kernel.api.labelscan.LabelScanStore;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LabelScanStoreCheckTaskTest
{
    @Test
    public void shouldUpdateProgress() throws Exception
    {
        // given
        ProgressMonitorFactory.MultiPartBuilder progressBuilder = mock( ProgressMonitorFactory.MultiPartBuilder.class );
        ProgressListener progressListener = mock( ProgressListener.class );
        when( progressBuilder.progressForPart( anyString(), anyLong() ) ).thenReturn( progressListener );

        LabelScanStore labelScanStore = mock( LabelScanStore.class );
        when(labelScanStore.newAllEntriesReader()).thenReturn( new AllEntriesLabelScanReader()
        {
            @Override
            public long getHighRangeId() throws IOException
            {
                return 2;
            }

            @Override
            public void close() throws IOException
            {
            }

            @Override
            public Iterator<NodeLabelRange> iterator()
            {
                ArrayList<NodeLabelRange> ranges = new ArrayList<NodeLabelRange>() {{
                    add(new StubNodeLabelRange(0));
                    add(new StubNodeLabelRange(1));
                    add(new StubNodeLabelRange(2));
                }};
                return ranges.iterator();
            }
        } );

        LabelScanStoreCheckTask task = new LabelScanStoreCheckTask( labelScanStore, progressBuilder, new NullReporter(),
                null);

        // when
        task.run();

        // then
        verify( progressListener ).set( 0 );
        verify( progressListener ).set( 1 );
        verify( progressListener ).set( 2 );
        verify( progressListener ).done();
    }

    private static class StubNodeLabelRange implements NodeLabelRange
    {
        private final int id;

        public StubNodeLabelRange( int id )
        {
            this.id = id;
        }

        @Override public int id()
        {
            return id;
        }

        @Override public long[] nodes()
        {
            return new long[0];
        }

        @Override public long[] labels( long nodeId )
        {
            return new long[0];
        }
    }
}
