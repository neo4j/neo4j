/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server.security.systemgraph;

import java.util.Map;
import java.util.Set;

import org.neo4j.cypher.result.QueryResult;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.auth.exception.FormatException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;

import static org.neo4j.helpers.collection.MapUtil.map;

public class BasicSystemGraphOperations
{
    protected final QueryExecutor queryExecutor;
    private final SecureHasher secureHasher;

    public BasicSystemGraphOperations( QueryExecutor queryExecutor, SecureHasher secureHasher )
    {
        this.queryExecutor = queryExecutor;
        this.secureHasher = secureHasher;
    }

    public void addUser( User user ) throws InvalidArgumentsException
    {
        // NOTE: If username already exists we will violate a constraint
        String query = "CREATE (u:User {name: $name, credentials: $credentials, passwordChangeRequired: $passwordChangeRequired, suspended: $suspended})";
        Map<String,Object> params =
                MapUtil.map( "name", user.name(),
                        "credentials", user.credentials().serialize(),
                        "passwordChangeRequired", user.passwordChangeRequired(),
                        "suspended", user.hasFlag( BasicSystemGraphRealm.IS_SUSPENDED ) );
        queryExecutor.executeQueryWithConstraint( query, params,
                "The specified user '" + user.name() + "' already exists." );
    }

    public Set<String> getAllUsernames()
    {
        String query = "MATCH (u:User) RETURN u.name";
        return queryExecutor.executeQueryWithResultSet( query );
    }

    public boolean deleteUser( String username ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $name}) DETACH DELETE u RETURN 0";
        Map<String,Object> params = map("name", username );
        String errorMsg = "User '" + username + "' does not exist.";

        return queryExecutor.executeQueryWithParamCheck( query, params, errorMsg );
    }

    public User getUser( String username, boolean silent ) throws InvalidArgumentsException
    {
        User[] user = new User[1];

        String query = "MATCH (u:User {name: $name}) RETURN u.credentials, u.passwordChangeRequired, u.suspended";
        Map<String,Object> params = map( "name", username );

        final QueryResult.QueryResultVisitor<FormatException> resultVisitor = row ->
        {
            AnyValue[] fields = row.fields();
            Credential credential = SystemGraphCredential.deserialize( ((TextValue) fields[0]).stringValue(), secureHasher );
            boolean requirePasswordChange = ((BooleanValue) fields[1]).booleanValue();
            boolean suspended = ((BooleanValue) fields[2]).booleanValue();

            if ( suspended )
            {
                user[0] = new User.Builder()
                        .withName( username )
                        .withCredentials( credential )
                        .withRequiredPasswordChange( requirePasswordChange )
                        .withFlag( BasicSystemGraphRealm.IS_SUSPENDED )
                        .build();
            }
            else
            {
                user[0] = new User.Builder()
                        .withName( username )
                        .withCredentials( credential )
                        .withRequiredPasswordChange( requirePasswordChange )
                        .withoutFlag( BasicSystemGraphRealm.IS_SUSPENDED )
                        .build();
            }

            return false;
        };

        queryExecutor.executeQuery( query, params, resultVisitor );

        if ( user[0] == null && !silent )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }

        return user[0];
    }

    protected void setUserCredentials( String username, String newCredentials, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $name}) SET u.credentials = $credentials, " +
                "u.passwordChangeRequired = $passwordChangeRequired RETURN u.name";
        Map<String,Object> params =
                map( "name", username,
                        "credentials", newCredentials,
                        "passwordChangeRequired", requirePasswordChange );
        String errorMsg = "User '" + username + "' does not exist.";

        queryExecutor.executeQueryWithParamCheck( query, params, errorMsg );
    }

}
