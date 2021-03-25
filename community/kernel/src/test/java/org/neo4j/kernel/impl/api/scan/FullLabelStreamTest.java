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
package org.neo4j.kernel.impl.api.scan;

import org.junit.jupiter.api.Test;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.memory.EmptyMemoryTracker;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class FullLabelStreamTest
{
    @Test
    void shouldCreateIndexStoreViewWithSingleThreadedWrite()
    {
        // given
        IndexStoreView indexStoreView = mock( IndexStoreView.class );
        FullLabelStream stream = new FullLabelStream( indexStoreView );

        // when
        stream.getStoreScan( indexStoreView, PageCacheTracer.NULL, EmptyMemoryTracker.INSTANCE );

        // then
        verify( indexStoreView ).visitNodes( any(), any(), any(), any(), anyBoolean(), eq( false ), any(), any() );
    }
}
