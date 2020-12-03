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

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.util.Preconditions;

import static java.lang.String.format;
import static org.neo4j.server.security.systemgraph.UserSecurityGraphComponentVersion.COMMUNITY_SECURITY_35;
import static org.neo4j.server.security.systemgraph.UserSecurityGraphComponentVersion.LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION;
import static org.neo4j.dbms.database.ComponentVersion.Neo4jVersions.UNKNOWN_VERSION;
import static org.neo4j.server.security.systemgraph.SystemGraphRealmHelper.IS_SUSPENDED;

/**
 * This is the UserSecurityComponent version for Neo4j 3.5
 */
public class CommunitySecurityComponentVersion_0_35 extends KnownCommunitySecurityComponentVersion
{
    private final UserRepository userRepository;

    public CommunitySecurityComponentVersion_0_35( Log log, UserRepository userRepository )
    {
        super( COMMUNITY_SECURITY_35, log );
        this.userRepository = userRepository;
    }

    @Override
    public boolean detected( Transaction tx )
    {
        if ( nodesWithLabelExist( tx, USER_LABEL ) || getVersion( tx ) != UNKNOWN_VERSION )
        {
            return false;
        }
        else
        {
            try
            {
                userRepository.start();
                return userRepository.numberOfUsers() > 0;
            }
            catch ( Exception e )
            {
                return false;
            }
        }
    }

    @Override
    public void setupUsers( Transaction tx )
    {
        throw unsupported();
    }

    @Override
    public void updateInitialUserPassword( Transaction tx )
    {
        throw unsupported();
    }

    @Override
    public void upgradeSecurityGraph( Transaction tx, KnownCommunitySecurityComponentVersion latest ) throws Exception
    {
        Preconditions.checkState( latest.version == LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION,
                format("Latest version should be %s but was %s", LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION, latest.version ));
        userRepository.start();
        ListSnapshot<User> users = userRepository.getSnapshot();

        if ( !users.values().isEmpty() )
        {
            for ( User user : users.values() )
            {
                addUser( tx, user.name(), user.credentials(), user.passwordChangeRequired(), user.hasFlag( IS_SUSPENDED ) );
            }

            // Log what happened to the security log
            String userString = users.values().size() == 1 ? "user" : "users";
            log.info( "Completed migration of %s %s into system graph.", Integer.toString( users.values().size() ), userString );
        }
        else
        {
            log.info( "No users migrated from auth file into system graph." );
        }
        setVersionProperty( tx, latest.version );
    }
}
