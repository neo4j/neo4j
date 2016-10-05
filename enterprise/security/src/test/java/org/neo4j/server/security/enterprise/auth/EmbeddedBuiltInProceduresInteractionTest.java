/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthSubject;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.DoubleLatch;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;

public class EmbeddedBuiltInProceduresInteractionTest extends BuiltInProceduresInteractionTestBase<EnterpriseAuthSubject>
{
    @Override
    protected NeoInteractionLevel<EnterpriseAuthSubject> setUpNeoServer( Map<String, String> config ) throws Throwable
    {
        return new EmbeddedInteraction( config );
    }

    @Test
    public void shouldNotListAnyQueriesIfNotAuthenticated()
    {
        GraphDatabaseFacade graph = neo.getLocalGraph();

        try ( InternalTransaction tx = graph
                .beginTransaction( KernelTransaction.Type.explicit, AccessMode.Static.FULL ) )
        {
            Result result = graph.execute( tx, "CALL dbms.listQueries", Collections.emptyMap() );
            assertFalse( result.hasNext() );
            tx.success();
        }
    }

    @Test
    public void shouldNotKillQueryIfNotAuthenticated() throws Throwable
    {
        EnterpriseAuthSubject authy = createFakeAnonymousEnterpriseAuthSubject();

        GraphDatabaseFacade graph = neo.getLocalGraph();
        DoubleLatch latch = new DoubleLatch( 2 );
        ThreadedTransaction<EnterpriseAuthSubject> read = new ThreadedTransaction<>( neo, latch );
        String query = read.execute( threading, authy, "UNWIND [1,2,3] AS x RETURN x" );

        latch.startAndWaitForAllToStart();

        String id = extractQueryId( query );

        try ( InternalTransaction tx = graph
                .beginTransaction( KernelTransaction.Type.explicit, AuthSubject.ANONYMOUS ) )
        {
            graph.execute( tx, "CALL dbms.killQuery('" + id + "')", Collections.emptyMap() );
            throw new AssertionError( "Expected exception to be thrown" );
        }
        catch ( QueryExecutionException e )
        {
            assertThat( e.getMessage(), containsString( PERMISSION_DENIED ) );
        }

        latch.finishAndWaitForAllToFinish();
        read.closeAndAssertSuccess();
    }

    private EnterpriseAuthSubject createFakeAnonymousEnterpriseAuthSubject()
    {
        return new EnterpriseAuthSubject()
        {
            @Override
            public boolean allowsReads()
            {
                return ANONYMOUS.allowsReads();
            }

            @Override
            public boolean allowsWrites()
            {
                return ANONYMOUS.allowsWrites();
            }

            @Override
            public boolean allowsSchemaWrites()
            {
                return ANONYMOUS.allowsSchemaWrites();
            }

            @Override
            public boolean overrideOriginalMode()
            {
                return ANONYMOUS.overrideOriginalMode();
            }

            @Override
            public AuthorizationViolationException onViolation( String msg )
            {
                return ANONYMOUS.onViolation( msg );
            }

            @Override
            public String name()
            {
                return ANONYMOUS.name();
            }

            @Override
            public boolean isAdmin()
            {
                return false;
            }

            @Override
            public void logout()
            {
                ANONYMOUS.logout();
            }

            @Override
            public AuthenticationResult getAuthenticationResult()
            {
                return ANONYMOUS.getAuthenticationResult();
            }

            @Override
            public void setPassword( String password, boolean requirePasswordChange )
                    throws IOException, InvalidArgumentsException
            {
                ANONYMOUS.setPassword( password, requirePasswordChange );

            }

            @Override
            public void passwordChangeNoLongerRequired()
            {
            }

            @Override
            public boolean allowsProcedureWith( String[] roleNames ) throws InvalidArgumentsException
            {
                return ANONYMOUS.allowsProcedureWith( roleNames );
            }

            @Override
            public String username()
            {
                return ANONYMOUS.username();
            }

            @Override
            public boolean hasUsername( String username )
            {
                return ANONYMOUS.hasUsername( username );
            }

            @Override
            public void ensureUserExistsWithName( String username ) throws InvalidArgumentsException
            {
                ANONYMOUS.ensureUserExistsWithName( username );
            }
        };
    }
}
