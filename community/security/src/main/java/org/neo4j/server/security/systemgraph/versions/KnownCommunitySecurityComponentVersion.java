/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.security.systemgraph.versions;

import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.cypher.internal.security.FormatException;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.cypher.internal.security.SystemGraphCredential;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.Log;
import org.neo4j.dbms.database.ComponentVersion;
import org.neo4j.dbms.database.KnownSystemComponentVersion;
import org.neo4j.string.UTF8;

import static org.neo4j.kernel.api.security.AuthManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;
import static org.neo4j.dbms.database.ComponentVersion.Neo4jVersions.UNKNOWN_VERSION;

public abstract class KnownCommunitySecurityComponentVersion extends KnownSystemComponentVersion
{
    public static final Label USER_LABEL = Label.label( "User" );
    private final SecureHasher secureHasher = new SecureHasher();

    KnownCommunitySecurityComponentVersion( ComponentVersion componentVersion, Log log )
    {
        super( componentVersion, log );
    }

    boolean componentNotInVersionNode( Transaction tx )
    {
        return getVersion( tx ) == UNKNOWN_VERSION;
    }

    public abstract void setupUsers( Transaction tx ) throws Exception;

    void addUser( Transaction tx, String username, Credential credentials, boolean passwordChangeRequired, boolean suspended )
    {
        // NOTE: If username already exists we will violate a constraint
        log.info( String.format( "Creating new user '%s' (passwordChangeRequired=%b, suspended=%b)", username, passwordChangeRequired, suspended ) );
        Node node = tx.createNode( USER_LABEL );
        node.setProperty( "name", username );
        node.setProperty( "credentials", credentials.serialize() );
        node.setProperty( "passwordChangeRequired", passwordChangeRequired );
        node.setProperty( "suspended", suspended );
    }

    public abstract void updateInitialUserPassword( Transaction tx ) throws Exception;

    void updateInitialUserPassword( Transaction tx, User initialUser ) throws FormatException
    {
        // The set-initial-password command should only take effect if the only existing user is the default user with the default password.
        ResourceIterator<Node> nodes = tx.findNodes( USER_LABEL );
        List<Node> users = nodes.stream().collect( Collectors.toList() );
        if ( users.size() == 0 )
        {
            log.warn( String.format( "Unable to update missing initial user password from `auth.ini` file: %s", initialUser.name() ) );
        }
        else if ( users.size() == 1 )
        {
            Node user = users.get( 0 );
            if ( user.getProperty( "name" ).equals( INITIAL_USER_NAME ) )
            {
                SystemGraphCredential currentCredentials = SystemGraphCredential.deserialize( user.getProperty( "credentials" ).toString(), secureHasher );
                if ( currentCredentials.matchesPassword( UTF8.encode( INITIAL_PASSWORD ) ) )
                {
                    log.info( "Updating initial user password from `auth.ini` file: %s", initialUser.name() );
                    user.setProperty( "credentials", initialUser.credentials().serialize() );
                    user.setProperty( "passwordChangeRequired", initialUser.passwordChangeRequired() );
                }
            }
        }
        else
        {
            log.error( String.format( "Multiple users matching initial user password from `auth.ini` file: %s", initialUser.name() ) );
        }
    }

    public void upgradeSecurityGraph( Transaction tx, KnownCommunitySecurityComponentVersion latest ) throws Exception
    {
        throw unsupported();
    }
}
