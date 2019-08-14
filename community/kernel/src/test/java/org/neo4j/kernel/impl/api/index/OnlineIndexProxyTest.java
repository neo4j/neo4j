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
package org.neo4j.kernel.impl.api.index;

import org.junit.jupiter.api.Test;

import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class OnlineIndexProxyTest
{
    private final long indexId = 1;
    private final IndexDescriptor descriptor = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 1, 2 ) ).withName( "index" ).materialise( indexId );
    private final IndexAccessor accessor = mock( IndexAccessor.class );
    private final IndexStoreView storeView = mock( IndexStoreView.class );
    private final IndexStatisticsStore indexStatisticsStore = mock( IndexStatisticsStore.class );

    @Test
    void shouldRemoveIndexCountsWhenTheIndexItselfIsDropped()
    {
        // given
        OnlineIndexProxy index = new OnlineIndexProxy( descriptor, accessor, indexStatisticsStore, false );

        // when
        index.drop();

        // then
        verify( accessor ).drop();
        verify( indexStatisticsStore ).removeIndex( indexId );
        verifyNoMoreInteractions( accessor, storeView );
    }
}
