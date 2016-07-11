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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Procedure.Mode.DBMS;

public class AuthProcedures
{
    public static final String PERMISSION_DENIED = "Permission denied";

    @Context
    public AuthSubject authSubject;

    @Context
    public GraphDatabaseAPI graph;

    @Procedure( name = "dbms.createUser", mode = DBMS )
    public void createUser( @Name( "username" ) String username, @Name( "password" ) String password,
            @Name( "requirePasswordChange" ) boolean requirePasswordChange )
            throws InvalidArgumentsException, IOException
    {
        EnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        adminSubject.getUserManager().newUser( username, password, requirePasswordChange );
    }

    @Procedure( name = "dbms.changeUserPassword", mode = DBMS )
    public void changeUserPassword( @Name( "username" ) String username, @Name( "newPassword" ) String newPassword )
            throws InvalidArgumentsException, IOException
    {
        EnterpriseAuthSubject enterpriseSubject = EnterpriseAuthSubject.castOrFail( authSubject );
        if ( enterpriseSubject.doesUsernameMatch( username ) )
        {
            enterpriseSubject.setPassword( newPassword );
        }
        else if ( !enterpriseSubject.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        else
        {
            enterpriseSubject.getUserManager().setUserPassword( username, newPassword );
        }
    }

    @Procedure( name = "dbms.addUserToRole", mode = DBMS )
    public void addUserToRole( @Name( "username" ) String username, @Name( "roleName" ) String roleName )
            throws IOException, InvalidArgumentsException
    {
        EnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        adminSubject.getUserManager().addUserToRole( username, roleName );
    }

    @Procedure( name = "dbms.removeUserFromRole", mode = DBMS )
    public void removeUserFromRole( @Name( "username" ) String username, @Name( "roleName" ) String roleName )
            throws InvalidArgumentsException, IOException
    {
        EnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        if ( adminSubject.doesUsernameMatch( username ) && roleName.equals( PredefinedRolesBuilder.ADMIN ) )
        {
            throw new InvalidArgumentsException( "Removing yourself from the admin role is not allowed!" );
        }
        adminSubject.getUserManager().removeUserFromRole( username, roleName );
    }

    @Procedure( name = "dbms.deleteUser", mode = DBMS )
    public void deleteUser( @Name( "username" ) String username ) throws InvalidArgumentsException, IOException
    {
        EnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        if ( adminSubject.doesUsernameMatch( username ) )
        {
            throw new InvalidArgumentsException( "Deleting yourself is not allowed!" );
        }
        adminSubject.getUserManager().deleteUser( username );
    }

    @Procedure( name = "dbms.suspendUser", mode = DBMS )
    public void suspendUser( @Name( "username" ) String username ) throws IOException, InvalidArgumentsException
    {
        EnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        if ( adminSubject.doesUsernameMatch( username ) )
        {
            throw new InvalidArgumentsException( "Suspending yourself is not allowed!" );
        }
        adminSubject.getUserManager().suspendUser( username );
    }

    @Procedure( name = "dbms.activateUser", mode = DBMS )
    public void activateUser( @Name( "username" ) String username ) throws IOException, InvalidArgumentsException
    {
        EnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        if ( adminSubject.doesUsernameMatch( username ) )
        {
            throw new InvalidArgumentsException( "Activating yourself is not allowed!" );
        }
        adminSubject.getUserManager().activateUser( username );
    }

    @Procedure( name = "dbms.showCurrentUser", mode = DBMS )
    public Stream<UserResult> showCurrentUser() throws InvalidArgumentsException, IOException
    {
        EnterpriseAuthSubject enterpriseSubject = EnterpriseAuthSubject.castOrFail( authSubject );
        EnterpriseUserManager userManager = enterpriseSubject.getUserManager();
        return Stream.of( new UserResult( enterpriseSubject.name(),
                userManager.getRoleNamesForUser( enterpriseSubject.name() ) ) );
    }

    @Procedure( name = "dbms.listUsers", mode = DBMS )
    public Stream<UserResult> listUsers() throws InvalidArgumentsException, IOException
    {
        EnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        EnterpriseUserManager userManager = adminSubject.getUserManager();
        Set<String> users = userManager.getAllUsernames();
        List<UserResult> results = new ArrayList<>();
        for ( String u : users )
        {
            results.add( new UserResult( u, userManager.getRoleNamesForUser( u ) ) );
        }
        return results.stream();
    }

    @Procedure( name = "dbms.listRoles", mode = DBMS )
    public Stream<RoleResult> listRoles() throws InvalidArgumentsException, IOException
    {
        EnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        EnterpriseUserManager userManager = adminSubject.getUserManager();
        Set<String> roles = userManager.getAllRoleNames();
        List<RoleResult> results = new ArrayList<>();
        for ( String r : roles )
        {
            results.add( new RoleResult( r, userManager.getUsernamesForRole( r ) ) );
        }
        return results.stream();
    }

    @Procedure( name = "dbms.listRolesForUser", mode = DBMS )
    public Stream<StringResult> listRolesForUser( @Name( "username" ) String username )
            throws InvalidArgumentsException, IOException
    {
        EnterpriseAuthSubject subject = EnterpriseAuthSubject.castOrFail( authSubject );
        if ( subject.isAdmin() || subject.doesUsernameMatch( username ) )
        {
            return subject.getUserManager().getRoleNamesForUser( username ).stream().map( StringResult::new );
        }
        throw new AuthorizationViolationException( PERMISSION_DENIED );
    }

    @Procedure( name = "dbms.listUsersForRole", mode = DBMS )
    public Stream<StringResult> listUsersForRole( @Name( "roleName" ) String roleName )
            throws InvalidArgumentsException, IOException
    {
        EnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        return adminSubject.getUserManager().getUsernamesForRole( roleName ).stream().map( StringResult::new );
    }

    @Procedure( name = "dbms.listTransactions", mode = DBMS )
    public Stream<TransactionResult> listTransactions()
            throws InvalidArgumentsException, IOException
    {
        ensureAdminAuthSubject();

        return countByUsername(
                    getActiveTransactions().stream()
                        .filter( tx -> tx.getReasonIfTerminated() == null )
                        .map( tx -> tx.mode().name() )
                );
    }

    @Procedure( name = "dbms.terminateTransactionsForUser", mode = DBMS )
    public Stream<TransactionResult> terminateTransactionsForUser( @Name( "username" ) String username )
            throws InvalidArgumentsException, IOException
    {
        EnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        //check that the user exists
        adminSubject.getUserManager().getUser( username );

        Long killCount = 0L;
        for ( KernelTransaction tx : getActiveTransactions() )
        {
            if ( tx.mode().name().equals( username ) )
            {
                tx.markForTermination( Status.Transaction.Terminated );
                killCount += 1;
            }
        }
        return Stream.of( new TransactionResult( username, killCount ) );
    }

    // ----------------- helpers ---------------------

    private Set<KernelTransaction> getActiveTransactions()
    {
        return graph.getDependencyResolver().resolveDependency( KernelTransactions.class ).activeTransactions();
    }

    private Stream<TransactionResult> countByUsername( Stream<String> usernames )
    {
        return usernames.collect(
                    Collectors.groupingBy( Function.identity(), Collectors.counting() )
                ).entrySet().stream().map(
                    entry -> new TransactionResult( entry.getKey(), entry.getValue() )
                );
    }

    private EnterpriseAuthSubject ensureAdminAuthSubject()
    {
        EnterpriseAuthSubject enterpriseAuthSubject = EnterpriseAuthSubject.castOrFail( authSubject );
        if ( !enterpriseAuthSubject.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        return enterpriseAuthSubject;
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

        UserResult( String username, Set<String> roles )
        {
            this.username = username;
            this.roles = new ArrayList<>();
            this.roles.addAll( roles );
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
        public final Long transactionCount;

        TransactionResult( String username, Long transactionCount )
        {
            this.username = username;
            this.transactionCount = transactionCount;
        }
    }
}
