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
package org.neo4j.storageengine.util;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IdGeneratorUpdatesWorkSyncTest
{
    @Test
    void shouldGatherChangedIds() throws ExecutionException
    {
        // given
        IdGeneratorUpdatesWorkSync workSync = new IdGeneratorUpdatesWorkSync();
        IdGenerator idGenerator = mock( IdGenerator.class );
        IdGenerator.Marker marker = mock( IdGenerator.Marker.class );
        when( idGenerator.marker( any() ) ).thenReturn( marker );
        workSync.add( idGenerator );

        // when
        IdGeneratorUpdatesWorkSync.Batch batch = workSync.newBatch( PageCacheTracer.NULL );
        batch.markIdAsUsed( idGenerator, 10, 1, CursorContext.NULL );
        batch.markIdAsUnused( idGenerator, 11, 1, CursorContext.NULL );
        batch.markIdAsUsed( idGenerator, 270, 4, CursorContext.NULL );
        batch.markIdAsUnused( idGenerator, 513, 7, CursorContext.NULL );
        batch.apply( PageCacheTracer.NULL );

        // then
        verify( marker ).markUsed( 10, 1 );
        verify( marker ).markDeleted( 11, 1 );
        verify( marker ).markUsed( 270, 4 );
        verify( marker ).markDeleted( 513, 7 );
    }
}
