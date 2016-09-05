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
package org.neo4j.kernel.enterprise.builtinprocs;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.ExecutingQuery;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.bolt.ManagedBoltStateMachine;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthSubject;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.time.Clocks;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.kernel.impl.api.security.OverriddenAccessMode.getUsernameFromAccessMode;
import static org.neo4j.procedure.Mode.DBMS;

public class BuiltInProcedures
{
    public static Clock clock = Clocks.systemClock();

    @Context
    public DependencyResolver resolver;

    @Context
    public GraphDatabaseAPI graph;

    @Context
    public KernelTransaction tx;

    @Context
    public AuthSubject authSubject;

    @Procedure( name = "dbms.listTransactions", mode = DBMS )
    public Stream<TransactionResult> listTransactions()
            throws InvalidArgumentsException, IOException
    {
        ensureAdminEnterpriseAuthSubject();

        return countTransactionByUsername(
            getActiveTransactions()
                .stream()
                .filter( tx -> !tx.terminationReason().isPresent() )
                .map( tx -> getUsernameFromAccessMode( tx.mode() ) )
        );
    }

    @Procedure( name = "dbms.terminateTransactionsForUser", mode = DBMS )
    public Stream<TransactionTerminationResult> terminateTransactionsForUser( @Name( "username" ) String username )
            throws InvalidArgumentsException, IOException
    {
        ensureSelfOrAdminEnterpriseAuthSubject( username );

        return terminateTransactionsForValidUser( username );
    }

    @Procedure( name = "dbms.listConnections", mode = DBMS )
    public Stream<ConnectionResult> listConnections()
    {
        ensureAdminEnterpriseAuthSubject();

        BoltConnectionTracker boltConnectionTracker = getBoltConnectionTracker();
        return countConnectionsByUsername(
            boltConnectionTracker
                .getActiveConnections()
                .stream()
                .filter( session -> !session.hasTerminated() )
                .map( ManagedBoltStateMachine::owner )
        );
    }

    @Procedure( name = "dbms.terminateConnectionsForUser", mode = DBMS )
    public Stream<ConnectionResult> terminateConnectionsForUser( @Name( "username" ) String username )
            throws InvalidArgumentsException
    {
        EnterpriseAuthSubject subject = EnterpriseAuthSubject.castOrFail( authSubject );
        if ( !subject.isAdmin() && !subject.hasUsername( username ) )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }

        subject.ensureUserExistsWithName( username );

        return terminateConnectionsForValidUser( username );
    }

    @Procedure( name = "dbms.listQueries", mode = DBMS )
    public Stream<QueryStatusResult> listQueries() throws InvalidArgumentsException, IOException
    {
        return getKernelTransactions()
            .activeTransactions()
            .stream()
            .flatMap( KernelTransactionHandle::executingQueries )
            .filter( ( query ) -> isAdminEnterpriseAuthSubject() || authSubject.hasUsername( query.authSubjectName() ) )
            .map( this::queryStatusResult );
    }

    private KernelTransactions getKernelTransactions()
    {
        return resolver.resolveDependency( KernelTransactions.class );
    }

    // ----------------- helpers ---------------------

    private Stream<TransactionTerminationResult> terminateTransactionsForValidUser( String username )
    {
        long terminatedCount = 0;
        for ( KernelTransactionHandle tx : getActiveTransactions() )
        {
            if ( getUsernameFromAccessMode( tx.mode() ).equals( username ) && !tx.isUnderlyingTransaction( this.tx ) )
            {
                boolean marked = tx.markForTermination( Status.Transaction.Terminated );
                if ( marked )
                {
                    terminatedCount++;
                }
            }
        }
        return Stream.of( new TransactionTerminationResult( username, terminatedCount ) );
    }

    private Stream<ConnectionResult> terminateConnectionsForValidUser( String username )
    {
        Long killCount = 0L;
        for ( ManagedBoltStateMachine connection : getBoltConnectionTracker().getActiveConnections( username ) )
        {
            connection.terminate();
            killCount += 1;
        }
        return Stream.of( new ConnectionResult( username, killCount ) );
    }

    private Set<KernelTransactionHandle> getActiveTransactions()
    {
        return graph.getDependencyResolver().resolveDependency( KernelTransactions.class ).activeTransactions();
    }

    private BoltConnectionTracker getBoltConnectionTracker()
    {
        return graph.getDependencyResolver().resolveDependency( BoltConnectionTracker.class );
    }

    private Stream<TransactionResult> countTransactionByUsername( Stream<String> usernames )
    {
        return usernames
            .collect( Collectors.groupingBy( Function.identity(), Collectors.counting() ) )
            .entrySet()
            .stream()
            .map( entry -> new TransactionResult( entry.getKey(), entry.getValue() )
        );
    }

    private Stream<ConnectionResult> countConnectionsByUsername( Stream<String> usernames )
    {
        return usernames
            .collect( Collectors.groupingBy( Function.identity(), Collectors.counting() ) )
            .entrySet()
            .stream()
            .map( entry -> new ConnectionResult( entry.getKey(), entry.getValue() )
        );
    }

    private boolean isAdminEnterpriseAuthSubject()
    {
        if ( authSubject instanceof EnterpriseAuthSubject )
        {
            EnterpriseAuthSubject enterpriseAuthSubject = (EnterpriseAuthSubject) authSubject;
            return enterpriseAuthSubject.isAdmin();
        }
        else
        {
            return false;
        }
    }

    private EnterpriseAuthSubject ensureAdminEnterpriseAuthSubject()
    {
        EnterpriseAuthSubject enterpriseAuthSubject = EnterpriseAuthSubject.castOrFail( authSubject );
        if ( !enterpriseAuthSubject.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        return enterpriseAuthSubject;
    }

    private EnterpriseAuthSubject ensureSelfOrAdminEnterpriseAuthSubject( String username )
            throws InvalidArgumentsException
    {
        EnterpriseAuthSubject subject = EnterpriseAuthSubject.castOrFail( authSubject );
        subject.ensureUserExistsWithName( username );

        if ( subject.isAdmin() || subject.hasUsername( username ) )
        {
            return subject;
        }
        throw new AuthorizationViolationException( PERMISSION_DENIED );
    }

    private QueryStatusResult queryStatusResult( ExecutingQuery q )
    {
        return new QueryStatusResult(
            q.queryId(),
            q.authSubjectName(),
            q.queryText(),
            q.queryParameters(),
            q.startTime(),
            clock.instant().minusMillis( q.startTime() ).toEpochMilli()
        );
    }

    public static class QueryStatusResult
    {
        public final long queryId;

        public final String username;
        public final String query;
        public final Map<String,Object> parameters;
        public final String startTime;
        public final String elapsedTime;

        QueryStatusResult( long queryId, String username, String query, Map<String,Object> parameters,
                long startTime, long elapsedTime )
        {
            this.queryId = queryId;
            this.username = username;
            this.query = query;
            this.parameters = parameters;
            this.startTime = formatTime( startTime );
            this.elapsedTime = formatInterval( elapsedTime );
        }

        private static String formatTime( final long startTime )
        {
            return OffsetDateTime
                .ofInstant( Instant.ofEpochMilli( startTime ), ZoneId.systemDefault() )
                .format( ISO_OFFSET_DATE_TIME );
        }
    }

    public static class TransactionResult
    {
        public final String username;
        public final Long activeTransactions;

        TransactionResult( String username, Long activeTransactions )
        {
            this.username = username;
            this.activeTransactions = activeTransactions;
        }
    }

    public static class TransactionTerminationResult
    {
        public final String username;
        public final Long transactionsTerminated;

        TransactionTerminationResult( String username, Long transactionsTerminated )
        {
            this.username = username;
            this.transactionsTerminated = transactionsTerminated;
        }
    }

    public static class ConnectionResult
    {
        public final String username;
        public final Long connectionCount;

        ConnectionResult( String username, Long connectionCount )
        {
            this.username = username;
            this.connectionCount = connectionCount;
        }
    }

    static String formatInterval( final long l )
    {
        final long hr = MILLISECONDS.toHours( l );
        final long min = MILLISECONDS.toMinutes( l - HOURS.toMillis( hr ) );
        final long sec = MILLISECONDS.toSeconds( l - HOURS.toMillis( hr ) - MINUTES.toMillis( min ) );
        final long ms = l - HOURS.toMillis( hr ) - MINUTES.toMillis( min ) - SECONDS.toMillis( sec );
        return String.format( "%02d:%02d:%02d.%03d", hr, min, sec, ms );
    }
}
