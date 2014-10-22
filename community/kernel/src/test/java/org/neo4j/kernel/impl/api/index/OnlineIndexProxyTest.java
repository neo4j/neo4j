/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.MAX_TX_ID;

import java.io.IOException;

import org.junit.Test;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;

public class OnlineIndexProxyTest
{
    private final IndexDescriptor descriptor = new IndexDescriptor( 1, 2 );
    private final SchemaIndexProvider.Descriptor providerDescriptor = mock( SchemaIndexProvider.Descriptor.class );
    private final IndexAccessor indexAccessor = mock( IndexAccessor.class );
    private final IndexStoreView storeView = mock( IndexStoreView.class );

    @Test
    public void shouldRemoveIndexCountsWhenTheIndexItselfIsDropped() throws IOException
    {
        // given
        OnlineIndexProxy index = new OnlineIndexProxy( descriptor, providerDescriptor, indexAccessor, storeView );

        // when
        index.drop();

        // then
        verify( indexAccessor ).drop();
        verify( storeView ).replaceIndexSize( MAX_TX_ID, descriptor, 0l );
        verify( storeView ).replaceIndexSample( MAX_TX_ID, descriptor, 0l, 0l );
        verifyNoMoreInteractions( indexAccessor, storeView );
    }
}
