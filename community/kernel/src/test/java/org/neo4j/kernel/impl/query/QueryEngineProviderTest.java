/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.query;

import org.junit.Test;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryEngineProviderTest
{
    @Test
    public void shouldUsePickTheEngineWithLowestPriority() throws Throwable
    {
        // Given
        QueryEngineProvider provider1 = mock( QueryEngineProvider.class );
        QueryEngineProvider provider2 = mock( QueryEngineProvider.class );
        when( provider1.enginePriority() ).thenReturn( 1 );
        when( provider2.enginePriority() ).thenReturn( 2 );
        Dependencies deps = new Dependencies();
        GraphDatabaseAPI graphAPI = mock( GraphDatabaseAPI.class );
        QueryExecutionEngine executionEngine = mock( QueryExecutionEngine.class );
        QueryExecutionEngine executionEngine2 = mock( QueryExecutionEngine.class );
        when( provider1.createEngine( any(), any() ) ).thenReturn( executionEngine );
        when( provider2.createEngine( any(), any() ) ).thenReturn( executionEngine2 );

        // When
        Iterable<QueryEngineProvider> providers = Iterables.asIterable( provider1, provider2 );
        QueryExecutionEngine engine = QueryEngineProvider.initialize( deps, graphAPI, providers );

        // Then
        assertSame( executionEngine, engine );
    }

    @Test
    public void shouldPickTheOneAndOnlyQueryEngineAvailable() throws Throwable
    {
        // Given
        QueryEngineProvider provider = mock( QueryEngineProvider.class );
        when( provider.enginePriority() ).thenReturn( 1 );
        Dependencies deps = new Dependencies();
        GraphDatabaseAPI graphAPI = mock( GraphDatabaseAPI.class );
        QueryExecutionEngine executionEngine = mock( QueryExecutionEngine.class );
        when( provider.createEngine( any(), any() ) ).thenReturn( executionEngine );

        // When
        Iterable<QueryEngineProvider> providers = Iterables.asIterable( provider );
        QueryExecutionEngine engine = QueryEngineProvider.initialize( deps, graphAPI, providers );

        // Then
        assertSame( executionEngine, engine );
    }
}
