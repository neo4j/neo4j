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
package org.neo4j.server.http.cypher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.security.Principal;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LegacyTransactionServiceTest
{

    private HttpServletRequest request;
    private DefaultDatabaseResolver databaseResolver;
    private HttpTransactionManager httpTransactionManager;
    private UriInfo uriInfo;
    private Log log;

    @BeforeEach
    void prepareDependencies()
    {
        request = mock( HttpServletRequest.class );
        databaseResolver = mock( DefaultDatabaseResolver.class );
        httpTransactionManager = spy( new HttpTransactionManager(
                null, mock( JobScheduler.class ), Clock.systemUTC(), Duration.ofMinutes( 2 ), mock( LogProvider.class ) ) );
        uriInfo = mock( UriInfo.class );
        log = mock( Log.class );

        doReturn( UriBuilder.fromUri( "http://localhost:7474/db/data/transaction" ) )
                .when( uriInfo )
                .getBaseUriBuilder();

        doReturn( Optional.empty() )
                .when( httpTransactionManager )
                .getGraphDatabaseAPI( anyString() );
    }

    @ParameterizedTest
    @CsvSource( "neo4j, andy, greg" )
    void shouldSwitchDefaultDatabaseBasedOnAuthenticatedUser( String user )
    {
        // Given
        var userPrincipalA = mock( Principal.class );

        when( userPrincipalA.getName() )
                .thenReturn( user );

        when( request.getUserPrincipal() )
                .thenReturn( userPrincipalA );

        when( databaseResolver.defaultDatabase( anyString() ) )
                .thenReturn( "neo4j" );
        when( databaseResolver.defaultDatabase( user ) )
                .thenReturn( "db-" + user );

        // When
        var transactionService = new LegacyTransactionService( request, databaseResolver, httpTransactionManager, uriInfo, log );
        transactionService.rollbackTransaction( 42 );

        // Verify
        verify( databaseResolver )
                .defaultDatabase( user );

        verify( httpTransactionManager ).getGraphDatabaseAPI( "db-" + user );
    }
}
