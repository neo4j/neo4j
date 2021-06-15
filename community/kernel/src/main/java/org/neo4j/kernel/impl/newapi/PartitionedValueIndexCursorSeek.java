/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;

public class PartitionedValueIndexCursorSeek<Cursor extends org.neo4j.internal.kernel.api.Cursor> implements PartitionedScan<Cursor>
{
    private final Read read;
    private final PartitionedValueSeek valueSeek;
    private final PropertyIndexQuery[] query;
    private final IndexDescriptor descriptor;

    PartitionedValueIndexCursorSeek( Read read, IndexDescriptor descriptor, PartitionedValueSeek valueSeek, PropertyIndexQuery... query )
    {
        this.read = read;
        this.descriptor = descriptor;
        this.valueSeek = valueSeek;
        this.query = query;
        throwIfTxState();
    }

    @Override
    public int getNumberOfPartitions()
    {
        return valueSeek.getNumberOfPartitions();
    }

    @Override
    public boolean reservePartition( Cursor cursor )
    {
        throwIfTxState();
        final var indexCursor = (DefaultEntityValueIndexCursor<?>) cursor;
        indexCursor.setRead( read );
        final var indexProgressor = valueSeek.reservePartition( indexCursor );
        if ( indexProgressor == IndexProgressor.EMPTY )
        {
            return false;
        }
        indexCursor.initialize( descriptor, indexProgressor, query, IndexQueryConstraints.unorderedValues(), false );
        return true;
    }

    protected void throwIfTxState()
    {
        if ( read.hasTxStateWithChanges() )
        {
            throw new IllegalStateException( "Transaction contains changes; PartitionScan is only valid in Read-Only transactions." );
        }
    }
}
