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

import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.impl.index.schema.PartitionedTokenScan;

public class PartitionedTokenIndexCursorScan<Cursor extends org.neo4j.internal.kernel.api.Cursor> implements PartitionedScan<Cursor>
{
    private final Read read;
    private final TokenPredicate query;
    private final PartitionedTokenScan tokenScan;

    PartitionedTokenIndexCursorScan( Read read, TokenPredicate query, PartitionedTokenScan tokenScan )
    {
        if ( read.hasTxStateWithChanges() )
        {
            throw new IllegalStateException( "Transaction contains changes; PartitionScan is only valid in Read-Only transactions." );
        }
        this.read = read;
        this.query = query;
        this.tokenScan = tokenScan;
    }

    @Override
    public int getNumberOfPartitions()
    {
        return tokenScan.getNumberOfPartitions();
    }

    @Override
    public boolean reservePartition( Cursor cursor, CursorContext cursorContext, AccessMode accessMode )
    {
        var indexCursor = (DefaultEntityTokenIndexCursor<? extends DefaultEntityTokenIndexCursor<?>>) cursor;
        indexCursor.setRead( read );
        var indexProgressor = tokenScan.reservePartition( indexCursor, cursorContext );
        if ( indexProgressor == IndexProgressor.EMPTY )
        {
            return false;
        }
        indexCursor.initialize( indexProgressor, query.tokenId(), null, null, accessMode );
        return true;
    }
}
