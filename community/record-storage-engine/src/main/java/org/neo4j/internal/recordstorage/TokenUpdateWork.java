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
package org.neo4j.internal.recordstorage;

import java.util.List;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityTokenUpdateListener;
import org.neo4j.util.concurrent.Work;

import static org.neo4j.storageengine.api.EntityTokenUpdate.SORT_BY_ENTITY_ID;

public class TokenUpdateWork implements Work<EntityTokenUpdateListener,TokenUpdateWork>
{
    private final List<EntityTokenUpdate> tokenUpdates;
    private final PageCursorTracer cursorTracer;

    TokenUpdateWork( List<EntityTokenUpdate> tokenUpdates, PageCursorTracer cursorTracer )
    {
        this.tokenUpdates = tokenUpdates;
        this.cursorTracer = cursorTracer;
    }

    @Override
    public TokenUpdateWork combine( TokenUpdateWork work )
    {
        tokenUpdates.addAll( work.tokenUpdates );
        return this;
    }

    @Override
    public void apply( EntityTokenUpdateListener listener )
    {
        tokenUpdates.sort( SORT_BY_ENTITY_ID );
        listener.applyUpdates( tokenUpdates, cursorTracer );
    }
}
