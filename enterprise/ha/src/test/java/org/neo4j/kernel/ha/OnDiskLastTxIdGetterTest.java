/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.junit.Test;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.ha.transaction.OnDiskLastTxIdGetter;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;

public class OnDiskLastTxIdGetterTest
{
    @Test
    public void testGetLastTxIdNoFilePresent() throws Exception
    {
        // This is a sign that we have some bad coupling on our hands.
        // We currently have to do this because of our lifecycle and construction ordering.
        InternalAbstractGraphDatabase graphdb = mock( InternalAbstractGraphDatabase.class );
        DependencyResolver resolver = mock( DependencyResolver.class );
        NeoStoreProvider provider = mock( NeoStoreProvider.class );
        NeoStore neoStore = mock( NeoStore.class );
        when( graphdb.getDependencyResolver() ).thenReturn( resolver );
        when( resolver.resolveDependency( NeoStoreProvider.class ) ).thenReturn( provider );
        when( provider.evaluate() ).thenReturn( neoStore );
        when( neoStore.getLastCommittedTransactionId() ).thenReturn( 13L );

        OnDiskLastTxIdGetter getter = new OnDiskLastTxIdGetter( graphdb );
        assertEquals( 13L, getter.getLastTxId() );
    }
}
