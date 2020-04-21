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
package org.neo4j.server.security.systemgraph;

import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.AbstractSystemGraphComponent;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.versions.CommunityVersion_0_35;
import org.neo4j.server.security.systemgraph.versions.CommunityVersion_1_40;
import org.neo4j.server.security.systemgraph.versions.CommunityVersion_2_41d2;
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion;
import org.neo4j.server.security.systemgraph.versions.NoUserSecurityGraph;

import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_LABEL;

public class UserSecurityGraphComponent extends AbstractSystemGraphComponent
{
    public static final String COMPONENT = "security-users";
    private final KnownSystemComponentVersions<KnownCommunitySecurityComponentVersion> knownUserSecurityComponentVersions =
            new KnownSystemComponentVersions<>( new NoUserSecurityGraph() );

    public UserSecurityGraphComponent( Log log, UserRepository userRepository, UserRepository initialPasswordRepo, Config config )
    {
        super( config );
        knownUserSecurityComponentVersions.add( new CommunityVersion_0_35( log, userRepository ) );
        knownUserSecurityComponentVersions.add( new CommunityVersion_1_40( log, initialPasswordRepo ) );
        knownUserSecurityComponentVersions.add( new CommunityVersion_2_41d2( log, initialPasswordRepo ) );
    }

    @Override
    public String component()
    {
        return COMPONENT;
    }

    @Override
    public Status detect( Transaction tx )
    {
        return knownUserSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx ).getStatus();
    }

    @Override
    public void initializeSystemGraphModel( Transaction tx ) throws Exception
    {
        initializeLatestSystemGraph( tx );
    }

    @Override
    public void initializeSystemGraphConstraints( Transaction tx )
    {
        initializeSystemGraphConstraint( tx, USER_LABEL, "name" );
    }

    private void initializeLatestSystemGraph( Transaction tx ) throws Exception
    {
        KnownCommunitySecurityComponentVersion latest = knownUserSecurityComponentVersions.latestSecurityGraphVersion();
        latest.setupUsers( tx );
        latest.setVersionProperty( tx, latest.version );
    }

    @Override
    protected void postInitialization( GraphDatabaseService system, boolean wasInitialized ) throws Exception
    {
        if ( !wasInitialized )
        {
            try ( Transaction tx = system.beginTx() )
            {
                KnownCommunitySecurityComponentVersion component = knownUserSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx );
                Optional<Exception> exception = component.updateInitialUserPassword( tx );
                tx.commit();
                if ( exception.isPresent() )
                {
                    throw exception.get();
                }
            }
        }
    }

    @Override
    public Optional<Exception> upgradeToCurrent( Transaction tx )
    {
        KnownCommunitySecurityComponentVersion component = knownUserSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx );
        if ( component.version == NoUserSecurityGraph.VERSION )
        {
            try
            {
                initializeLatestSystemGraph( tx );
            }
            catch ( Exception e )
            {
                return Optional.of( e );
            }
        }
        else
        {
            if ( component.migrationSupported() )
            {
                try
                {
                    component.upgradeSecurityGraph( tx, knownUserSecurityComponentVersions.latestSecurityGraphVersion() );
                }
                catch ( Exception e )
                {
                    return Optional.of( e );
                }
            }
            else
            {
                return Optional.of( component.unsupported() );
            }
        }
        return Optional.empty();
    }
}
