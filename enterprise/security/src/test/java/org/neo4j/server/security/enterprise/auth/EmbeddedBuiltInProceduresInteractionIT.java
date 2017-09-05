/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.DoubleLatch;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public class EmbeddedBuiltInProceduresInteractionIT extends BuiltInProceduresInteractionTestBase<EnterpriseSecurityContext>
{

    @Override
    protected Object valueOf( Object obj )
    {
        if ( obj instanceof Integer )
        {
            return ((Integer) obj).longValue();
        }
        else
        {
            return obj;
        }
    }

    @Override
    protected NeoInteractionLevel<EnterpriseSecurityContext> setUpNeoServer( Map<String, String> config ) throws Throwable
    {
        return new EmbeddedInteraction( config );
    }

    @Test
    public void shouldNotListAnyQueriesIfNotAuthenticated()
    {
        GraphDatabaseFacade graph = neo.getLocalGraph();

        try ( InternalTransaction tx = graph
                .beginTransaction( KernelTransaction.Type.explicit, AnonymousContext.none() ) )
        {
            Result result = graph.execute( tx, "CALL dbms.listQueries", EMPTY_MAP );
            assertFalse( result.hasNext() );
            tx.success();
        }
    }

    @Test
    public void shouldNotKillQueryIfNotAuthenticated() throws Throwable
    {
        EnterpriseSecurityContext authy = createFakeAnonymousEnterpriseSecurityContext();

        GraphDatabaseFacade graph = neo.getLocalGraph();
        DoubleLatch latch = new DoubleLatch( 2 );
        ThreadedTransaction<EnterpriseSecurityContext> read = new ThreadedTransaction<>( neo, latch );
        String query = read.execute( threading, authy, "UNWIND [1,2,3] AS x RETURN x" );

        latch.startAndWaitForAllToStart();

        String id = extractQueryId( query );

        try ( InternalTransaction tx = graph
                .beginTransaction( KernelTransaction.Type.explicit, AnonymousContext.none() ) )
        {
            graph.execute( tx, "CALL dbms.killQuery('" + id + "')", EMPTY_MAP );
            throw new AssertionError( "Expected exception to be thrown" );
        }
        catch ( QueryExecutionException e )
        {
            assertThat( e.getMessage(), containsString( PERMISSION_DENIED ) );
        }

        latch.finishAndWaitForAllToFinish();
        read.closeAndAssertSuccess();
    }

    private EnterpriseSecurityContext createFakeAnonymousEnterpriseSecurityContext()
    {
        return new EnterpriseSecurityContext()
        {
            @Override
            public EnterpriseSecurityContext freeze()
            {
                return this;
            }

            @Override
            public EnterpriseSecurityContext withMode( AccessMode mode )
            {
                return new EnterpriseSecurityContext.Frozen( subject(), mode, roles(), isAdmin() );
            }

            @Override
            public Set<String> roles()
            {
                return Collections.emptySet();
            }

            AnonymousContext inner = AnonymousContext.none();

            @Override
            public AuthSubject subject()
            {
                return inner.subject();
            }

            @Override
            public AccessMode mode()
            {
                return inner.mode();
            }

            @Override
            public boolean isAdmin()
            {
                return false;
            }
        };
    }
}
