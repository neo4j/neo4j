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
package org.neo4j.kernel.impl.api.scan;

import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.StoreScan;

import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;

/**
 * {@link FullStoreChangeStream} using a {@link IndexStoreView} to get its data.
 */
public class FullLabelStream implements FullStoreChangeStream, Visitor<NodeLabelUpdate,IOException>
{
    private final IndexStoreView indexStoreView;
    private LabelScanWriter writer;
    private long count;

    public FullLabelStream( IndexStoreView indexStoreView )
    {
        this.indexStoreView = indexStoreView;
    }

    @Override
    public long applyTo( LabelScanWriter writer ) throws IOException
    {
        // Keep the write for using it in visit
        this.writer = writer;
        StoreScan<IOException> scan = indexStoreView.visitNodes( ArrayUtils.EMPTY_INT_ARRAY, ALWAYS_TRUE_INT, null, this, true );
        scan.run();
        return count;
    }

    @Override
    public boolean visit( NodeLabelUpdate update ) throws IOException
    {
        writer.write( update );
        count++;
        return false;
    }
}
