/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.index.schema.TokenScanReader;
import org.neo4j.storageengine.api.StorageEntityScanCursor;

/**
 * Entity id iterator used during index population when we go over entity ids indexed in a token scan store.
 */
class TokenScanViewIdIterator<CURSOR extends StorageEntityScanCursor> implements EntityIdIterator
{
    private final int[] tokenIds;
    private final TokenScanReader tokenScanReader;
    private final CURSOR entityCursor;
    private final PageCursorTracer cursorTracer;

    private PrimitiveLongResourceIterator idIterator;
    private long lastReturnedId = -1;

    TokenScanViewIdIterator( TokenScanReader tokenScanReader, int[] tokenIds, CURSOR entityCursor, PageCursorTracer cursorTracer )
    {
        this.tokenScanReader = tokenScanReader;
        this.entityCursor = entityCursor;
        this.cursorTracer = cursorTracer;
        this.idIterator = tokenScanReader.entitiesWithAnyOfTokens( tokenIds, cursorTracer );
        this.tokenIds = tokenIds;
    }

    @Override
    public void close()
    {
        idIterator.close();
    }

    @Override
    public boolean hasNext()
    {
        return idIterator.hasNext();
    }

    @Override
    public long next()
    {
        long next = idIterator.next();
        entityCursor.single( next );
        entityCursor.next();
        lastReturnedId = next;
        return next;
    }

    @Override
    public void invalidateCache()
    {
        this.idIterator.close();
        this.idIterator = tokenScanReader.entitiesWithAnyOfTokens( lastReturnedId, tokenIds, cursorTracer );
    }
}
