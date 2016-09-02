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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.bolt.ManagedBoltStateMachine;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.enterprise.SecurityLog;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static java.util.function.Predicate.isEqual;
import static java.lang.String.format;

import static org.neo4j.kernel.impl.api.security.OverriddenAccessMode.getUsernameFromAccessMode;
import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( "unused" )
public class AuthProcedures
{
    public static final String PERMISSION_DENIED = "Permission denied.";

    @Context
    public AuthSubject authSubject;

    @Context
    public GraphDatabaseAPI graph;

    @Context
    public KernelTransaction tx;

    @Context
    public SecurityLog securityLog;

    @Description( "Create a new user." )
    @Procedure( name = "dbms.security.createUser", mode = DBMS )
    public void createUser(
            @Name( "username" ) String username,
            @Name( "password" ) String password,
            @Name( value = "requirePasswordChange", defaultValue = "true"  ) boolean requirePasswordChange
    )
            throws InvalidArgumentsException, IOException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            adminSubject.getUserManager().newUser( username, password, requirePasswordChange );
            securityLog.info( authSubject, "created user `%s`", username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to create user `%s`: %s", username, e.getMessage() );
            throw e;
        }
    }

    @Description( "Change the current user's password." )
    @Procedure( name = "dbms.security.changeUserPassword", mode = DBMS )
    public void changeUserPassword(
            @Name( "username" ) String username,
            @Name( "newPassword" ) String newPassword,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange
    )
            throws InvalidArgumentsException, IOException
    {
        String ownOrOther = format("password for user `%s`", username );
        try
        {
            StandardEnterpriseAuthSubject enterpriseSubject = StandardEnterpriseAuthSubject.castOrFail( authSubject );
            if ( enterpriseSubject.doesUsernameMatch( username ) )
            {
                ownOrOther = "own password";
                enterpriseSubject.setPassword( newPassword, requirePasswordChange );
                securityLog.info( authSubject, "changed own password" );
            }
            else if ( !enterpriseSubject.isAdmin() )
            {
                throw new AuthorizationViolationException( PERMISSION_DENIED );
            }
            else
            {
                enterpriseSubject.getUserManager().setUserPassword( username, newPassword, requirePasswordChange );
                terminateTransactionsForValidUser( username );
                terminateConnectionsForValidUser( username );
                securityLog.info( authSubject, "changed password for user `%s`", username );
            }
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to change %s: %s", ownOrOther, e.getMessage() );
            throw e;
        }
    }

    @Description( "Assign a role to the user." )
    @Procedure( name = "dbms.security.addRoleToUser", mode = DBMS )
    public void addRoleToUser(@Name( "roleName" ) String roleName, @Name( "username" ) String username )
            throws IOException, InvalidArgumentsException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            adminSubject.getUserManager().addRoleToUser( roleName, username );
            securityLog.info( authSubject, "added role `%s` to user `%s`", roleName, username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to add role `%s` to user `%s`: %s",
                    roleName, username, e.getMessage() );
            throw e;
        }
    }

    @Description( "Unassign a role from the user." )
    @Procedure( name = "dbms.security.removeRoleFromUser", mode = DBMS )
    public void removeRoleFromUser( @Name( "roleName" ) String roleName, @Name( "username" ) String username )
            throws InvalidArgumentsException, IOException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            if ( adminSubject.doesUsernameMatch( username ) && roleName.equals( PredefinedRolesBuilder.ADMIN ) )
            {
                throw new InvalidArgumentsException(
                        "Removing yourself (user '" + username + "') from the admin role is not allowed." );
            }
            adminSubject.getUserManager().removeRoleFromUser( roleName, username );
            securityLog.info( authSubject, "removed role `%s` from user `%s`", roleName, username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to remove role `%s` from user `%s`: %s", roleName, username, e
                    .getMessage() );
            throw e;
        }
    }

    @Description( "Delete the specified user." )
    @Procedure( name = "dbms.security.deleteUser", mode = DBMS )
    public void deleteUser( @Name( "username" ) String username ) throws InvalidArgumentsException, IOException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            if ( adminSubject.doesUsernameMatch( username ) )
            {
                throw new InvalidArgumentsException( "Deleting yourself (user '" + username + "') is not allowed." );
            }
            adminSubject.getUserManager().deleteUser( username );
            securityLog.info( authSubject, "deleted user `%s`", username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to delete user `%s`: %s", username, e.getMessage() );
            throw e;
        }

        kickoutUser( username, "deletion" );
    }

    void kickoutUser( String username, String reason )
    {
        try
        {
            terminateTransactionsForValidUser( username );
            terminateConnectionsForValidUser( username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "failed to terminate running transaction and bolt connections for " +
                    "user `%s` following %s. %s", username, reason, e.getMessage() );
            throw e;
        }
    }

    @Description( "Suspend the specified user." )
    @Procedure( name = "dbms.security.suspendUser", mode = DBMS )
    public void suspendUser( @Name( "username" ) String username ) throws IOException, InvalidArgumentsException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            if ( adminSubject.doesUsernameMatch( username ) )
            {
                throw new InvalidArgumentsException( "Suspending yourself (user '" + username +
                        "') is not allowed." );
            }
            adminSubject.getUserManager().suspendUser( username );
            securityLog.info( authSubject, "suspended user `%s`", username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to suspend user `%s`: %s", username, e.getMessage() );
            throw e;
        }

        kickoutUser( username, "suspension" );
    }

    @Description( "Activate a suspended user." )
    @Procedure( name = "dbms.security.activateUser", mode = DBMS )
    public void activateUser(
            @Name( "username" ) String username,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange
    ) throws IOException, InvalidArgumentsException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            if ( adminSubject.doesUsernameMatch( username ) )
            {
                throw new InvalidArgumentsException( "Activating yourself (user '" + username +
                        "') is not allowed." );
            }
            adminSubject.getUserManager().activateUser( username, requirePasswordChange );
            securityLog.info( authSubject, "activated user `%s`", username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to activate user `%s`: %s", username, e.getMessage() );
            throw e;
        }
    }

    @Description( "Show the current user." )
    @Procedure( name = "dbms.security.showCurrentUser", mode = DBMS )
    public Stream<UserResult> showCurrentUser() throws InvalidArgumentsException, IOException
    {
        StandardEnterpriseAuthSubject enterpriseSubject = StandardEnterpriseAuthSubject.castOrFail( authSubject );
        EnterpriseUserManager userManager = enterpriseSubject.getUserManager();
        return Stream.of( new UserResult( enterpriseSubject.username(),
                userManager.getRoleNamesForUser( enterpriseSubject.username() ),
                userManager.getUser( enterpriseSubject.username() ).getFlags() ) );
    }

    @Description( "List all local users." )
    @Procedure( name = "dbms.security.listUsers", mode = DBMS )
    public Stream<UserResult> listUsers() throws InvalidArgumentsException, IOException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            EnterpriseUserManager userManager = adminSubject.getUserManager();
            Set<String> users = userManager.getAllUsernames();
            List<UserResult> results = new ArrayList<>();
            for ( String u : users )
            {
                results.add(
                        new UserResult( u, userManager.getRoleNamesForUser( u ), userManager.getUser( u ).getFlags() ) );
            }
            return results.stream();
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to list users. %s", e.getMessage() );
            throw e;
        }
    }

    @Description( "List all available roles." )
    @Procedure( name = "dbms.security.listRoles", mode = DBMS )
    public Stream<RoleResult> listRoles() throws InvalidArgumentsException, IOException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            EnterpriseUserManager userManager = adminSubject.getUserManager();
            Set<String> roles = userManager.getAllRoleNames();
            List<RoleResult> results = new ArrayList<>();
            for ( String r : roles )
            {
                results.add( new RoleResult( r, userManager.getUsernamesForRole( r ) ) );
            }
            return results.stream();
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to list roles. %s", e.getMessage() );
            throw e;
        }
    }

    @Description( "List all roles assigned to the specified user." )
    @Procedure( name = "dbms.security.listRolesForUser", mode = DBMS )
    public Stream<StringResult> listRolesForUser( @Name( "username" ) String username )
            throws InvalidArgumentsException, IOException
    {
        try
        {
            StandardEnterpriseAuthSubject subject = ensureSelfOrAdminAuthSubject( username );
            return subject.getUserManager().getRoleNamesForUser( username ).stream().map( StringResult::new );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to list roles for user `%s`. %s", username, e.getMessage() );
            throw e;
        }
    }

    @Description( "List all users currently assigned the specified role." )
    @Procedure( name = "dbms.security.listUsersForRole", mode = DBMS )
    public Stream<StringResult> listUsersForRole( @Name( "roleName" ) String roleName )
            throws InvalidArgumentsException, IOException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            return adminSubject.getUserManager().getUsernamesForRole( roleName ).stream().map( StringResult::new );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to list users for role `%s`. %s", roleName, e.getMessage() );
            throw e;
        }
    }

    @Description( "Create a new role." )
    @Procedure( name = "dbms.security.createRole", mode = DBMS)
    public void createRole( @Name( "roleName" ) String roleName )
        throws InvalidArgumentsException, IOException
    {
        try
        {
            ensureAdminAuthSubject().getUserManager().newRole( roleName );
            securityLog.info( authSubject, "created role `%s`", roleName );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to create role `%s`: %s", roleName, e.getMessage() );
            throw e;
        }
    }

    @Description( "Delete the specified role. Any role assignments will be removed." )
    @Procedure( name = "dbms.security.deleteRole", mode = DBMS)
    public void deleteRole( @Name( "roleName" ) String roleName )
        throws InvalidArgumentsException, IOException
    {
        try
        {
            ensureAdminAuthSubject().getUserManager().deleteRole( roleName );
            securityLog.info( authSubject, "deleted role `%s`", roleName );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to delete role `%s`. %s", roleName, e.getMessage() );
            throw e;
        }
    }

    // TODO: add security logging once listQueries PR is in
    // ====================================================== FROM HERE
    @Procedure( name = "dbms.security.listTransactions", mode = DBMS )
    public Stream<TransactionResult> listTransactions()
            throws InvalidArgumentsException, IOException
    {
        ensureAdminAuthSubject();

        return countTransactionByUsername(
                    getActiveTransactions().stream()
                        .filter( tx -> !tx.terminationReason().isPresent() )
                        .map( tx -> getUsernameFromAccessMode( tx.mode() ) )
                );
    }

    @Procedure( name = "dbms.security.terminateTransactionsForUser", mode = DBMS )
    public Stream<TransactionTerminationResult> terminateTransactionsForUser( @Name( "username" ) String username )
            throws InvalidArgumentsException, IOException
    {
        ensureSelfOrAdminAuthSubject( username );

        return terminateTransactionsForValidUser( username );
    }

    @Procedure( name = "dbms.security.listConnections", mode = DBMS )
    public Stream<ConnectionResult> listConnections()
    {
        ensureAdminAuthSubject();

        BoltConnectionTracker boltConnectionTracker = getBoltConnectionTracker();
        return countConnectionsByUsername(
                boltConnectionTracker.getActiveConnections().stream()
                        .filter( session -> !session.hasTerminated() )
                        .map( ManagedBoltStateMachine::owner )
                );
    }

    @Procedure( name = "dbms.security.terminateConnectionsForUser", mode = DBMS )
    public Stream<ConnectionResult> terminateConnectionsForUser( @Name( "username" ) String username )
            throws InvalidArgumentsException
    {
        StandardEnterpriseAuthSubject subject = StandardEnterpriseAuthSubject.castOrFail( authSubject );
        if ( !subject.isAdmin() && !subject.doesUsernameMatch( username ) )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }

        subject.getUserManager().getUser( username );

        return terminateConnectionsForValidUser( username );
    }
    // ====================================================== TO HERE

    // ----------------- helpers ---------------------

    Stream<TransactionTerminationResult> terminateTransactionsForValidUser( String username )
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

    Stream<ConnectionResult> terminateConnectionsForValidUser( String username )
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
        return usernames.collect(
                    Collectors.groupingBy( Function.identity(), Collectors.counting() )
                ).entrySet().stream().map(
                    entry -> new TransactionResult( entry.getKey(), entry.getValue() )
                );
    }

    private Stream<ConnectionResult> countConnectionsByUsername( Stream<String> usernames )
    {
        return usernames.collect(
                    Collectors.groupingBy( Function.identity(), Collectors.counting() )
                ).entrySet().stream().map(
                    entry -> new ConnectionResult( entry.getKey(), entry.getValue() )
                );
    }

    private StandardEnterpriseAuthSubject ensureAdminAuthSubject()
    {
        StandardEnterpriseAuthSubject enterpriseAuthSubject = StandardEnterpriseAuthSubject.castOrFail( authSubject );
        if ( !enterpriseAuthSubject.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        return enterpriseAuthSubject;
    }

    private StandardEnterpriseAuthSubject ensureSelfOrAdminAuthSubject( String username ) throws InvalidArgumentsException
    {
        StandardEnterpriseAuthSubject subject = StandardEnterpriseAuthSubject.castOrFail( authSubject );
        subject.getUserManager().getUser( username );

        if ( subject.isAdmin() || subject.doesUsernameMatch( username ) )
        {
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

    public static class QueryStatusResult
    {
        public final long queryId;

        public final String username;
        public final String query;
        public final Map<String, Object> parameters;
        public final long startTime;

        public QueryStatusResult(
                long queryId,
                String username,
                String query,
                Map<String,Object> parameters,
                long startTime )
        {
            this.queryId = queryId;
            this.username = username;
            this.query = query;
            this.parameters = parameters;
            this.startTime = startTime;
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
}
