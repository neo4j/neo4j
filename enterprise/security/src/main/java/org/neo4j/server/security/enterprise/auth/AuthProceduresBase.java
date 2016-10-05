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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.bolt.ManagedBoltStateMachine;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.kernel.impl.api.security.OverriddenAccessMode.getUsernameFromAccessMode;

public class AuthProceduresBase
{
    @Context
    public AuthSubject authSubject;

    @Context
    public GraphDatabaseAPI graph;

    @Context
    public SecurityLog securityLog;

    // ----------------- helpers ---------------------

    protected void terminateTransactionsForValidUser( String username )
    {
        KernelTransaction currentTx = getCurrentTx();
        getActiveTransactions()
                .stream()
                .filter( tx ->
                    getUsernameFromAccessMode( tx.mode() ).equals( username ) &&
                    !tx.isUnderlyingTransaction( currentTx )
                ).forEach( tx -> tx.markForTermination( Status.Transaction.Terminated ) );
    }

    protected void terminateConnectionsForValidUser( String username )
    {
        getBoltConnectionTracker().getActiveConnections( username ).forEach( ManagedBoltStateMachine::terminate );
    }

    private Set<KernelTransactionHandle> getActiveTransactions()
    {
        return graph.getDependencyResolver().resolveDependency( KernelTransactions.class ).activeTransactions();
    }

    private BoltConnectionTracker getBoltConnectionTracker()
    {
        return graph.getDependencyResolver().resolveDependency( BoltConnectionTracker.class );
    }

    private KernelTransaction getCurrentTx()
    {
        return graph.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class )
                .getKernelTransactionBoundToThisThread( true );
    }

    protected StandardEnterpriseAuthSubject ensureAdminAuthSubject()
    {
        StandardEnterpriseAuthSubject enterpriseAuthSubject = StandardEnterpriseAuthSubject.castOrFail( authSubject );
        if ( !enterpriseAuthSubject.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        return enterpriseAuthSubject;
    }

    protected StandardEnterpriseAuthSubject ensureSelfOrAdminAuthSubject( String username )
            throws InvalidArgumentsException
    {
        StandardEnterpriseAuthSubject subject = StandardEnterpriseAuthSubject.castOrFail( authSubject );

        if ( subject.isAdmin() || subject.hasUsername( username ) )
        {
            subject.getUserManager().getUser( username );
            return subject;
        }

        throw new AuthorizationViolationException( PERMISSION_DENIED );
    }

    public static class StringResult
    {
        public final String value;

        StringResult( String value )
        {
            this.value = value;
        }
    }

    public static class UserResult
    {
        public final String username;
        public final List<String> roles;
        public final List<String> flags;

        UserResult( String username, Set<String> roles, Iterable<String> flags )
        {
            this.username = username;
            this.roles = new ArrayList<>();
            this.roles.addAll( roles );
            this.flags = new ArrayList<>();
            for ( String f : flags ) {this.flags.add( f );}
        }
    }

    public static class RoleResult
    {
        public final String role;
        public final List<String> users;

        RoleResult( String role, Set<String> users )
        {
            this.role = role;
            this.users = new ArrayList<>();
            this.users.addAll( users );
        }
    }
}
