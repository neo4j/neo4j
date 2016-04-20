/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.internal;

import org.junit.Test;

import org.neo4j.graphdb.Result;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QuerySession;

import static java.util.Collections.EMPTY_MAP;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CypherStatementRunnerTest
{
    private final QueryExecutionEngine engine = mock( QueryExecutionEngine.class );
    private final SessionStateMachine ctx = mock( SessionStateMachine.class );

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCreateImplicitTxIfNoneExists() throws Exception
    {
        // Given
        when( engine.isPeriodicCommit( anyString() )).thenReturn( false );
        when( engine.executeQuery( anyString(), anyMap(), any( QuerySession.class ) ) ).thenReturn( mock( Result.class ) );
        when( ctx.hasTransaction() ).thenReturn( false );

        CypherStatementRunner cypherRunner = new CypherStatementRunner( engine );

        // When
        cypherRunner.run( ctx, "<query>", EMPTY_MAP );

        // Then
        verify( ctx ).createSession( any( GraphDatabaseQueryService.class ), any( PropertyContainerLocker.class ));
        verify( ctx ).hasTransaction();
        verify( ctx ).beginImplicitTransaction();
        verify( engine ).isPeriodicCommit( "<query>" );
        verify( engine ).queryService();
        verify( engine ).executeQuery( eq( "<query>" ), eq( EMPTY_MAP ), any( QuerySession.class ) );
        verifyNoMoreInteractions( engine, ctx );
    }
}
