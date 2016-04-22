/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.scan;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.junit.Assert.assertEquals;

public class InMemoryLabelScanStoreTest
{

    @Test
    public void highestAvailableNodeId() throws IOException
    {
        InMemoryLabelScanStore inMemoryLabelScanStore = new InMemoryLabelScanStore();
        populateIndex( inMemoryLabelScanStore );
        LabelScanReader labelScanReader = inMemoryLabelScanStore.newReader();
        assertEquals( 6L, labelScanReader.getHighestIndexedNodeId() );
    }

    private void populateIndex( InMemoryLabelScanStore inMemoryLabelScanStore ) throws IOException
    {
        try ( LabelScanWriter labelScanWriter = inMemoryLabelScanStore.newWriter() )
        {
            labelScanWriter.write( NodeLabelUpdate.labelChanges( 1L, new long[]{}, new long[]{1, 2} ) );
            labelScanWriter.write( NodeLabelUpdate.labelChanges( 2L, new long[]{}, new long[]{1, 2} ) );
            labelScanWriter.write( NodeLabelUpdate.labelChanges( 3L, new long[]{}, new long[]{1, 2} ) );
            labelScanWriter.write( NodeLabelUpdate.labelChanges( 4L, new long[]{}, new long[]{1, 2} ) );
            labelScanWriter.write( NodeLabelUpdate.labelChanges( 5L, new long[]{}, new long[]{1, 2} ) );
            labelScanWriter.write( NodeLabelUpdate.labelChanges( 5L, new long[]{1, 2}, new long[]{2} ) );
            labelScanWriter.write( NodeLabelUpdate.labelChanges( 6L, new long[]{}, new long[]{1, 2} ) );
            labelScanWriter.write( NodeLabelUpdate.labelChanges( 6L, new long[]{1, 2}, new long[]{2} ) );
            labelScanWriter.write( NodeLabelUpdate.labelChanges( 7L, new long[]{1, 2}, new long[]{} ) );
            labelScanWriter.write( NodeLabelUpdate.labelChanges( 8L, new long[]{1, 2}, new long[]{} ) );
        }
    }
}
