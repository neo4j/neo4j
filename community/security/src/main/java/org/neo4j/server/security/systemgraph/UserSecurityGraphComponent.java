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
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.versions.CommunityVersion_0_35;
import org.neo4j.server.security.systemgraph.versions.CommunityVersion_1_40;
import org.neo4j.server.security.systemgraph.versions.CommunityVersion_2_41;
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion;
import org.neo4j.server.security.systemgraph.versions.NoUserSecurityGraph;

import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_LABEL;

public class UserSecurityGraphComponent extends AbstractSystemGraphComponent
{
    public static final String COMPONENT = "security-users";
    private final KnownSystemComponentVersions<KnownCommunitySecurityComponentVersion> knownUserSecurityComponentVersions =
            new KnownSystemComponentVersions<>( new NoUserSecurityGraph() );
    private final Log log;

    public UserSecurityGraphComponent( Log log, UserRepository userRepository, UserRepository initialPasswordRepo, Config config )
    {
        super( config );
        this.log = log;
        knownUserSecurityComponentVersions.add( new CommunityVersion_0_35( log, userRepository ) );
        knownUserSecurityComponentVersions.add( new CommunityVersion_1_40( log, initialPasswordRepo ) );
        knownUserSecurityComponentVersions.add( new CommunityVersion_2_41( log, initialPasswordRepo ) );
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
        KnownCommunitySecurityComponentVersion componentBeforeInit = knownUserSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx );
        log.info( "Initializing system graph model for component '%s' with version %d and status %s",
                COMPONENT, componentBeforeInit.version, componentBeforeInit.getStatus() );
        initializeLatestSystemGraph( tx );
        KnownCommunitySecurityComponentVersion componentAfterInit = knownUserSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx );
        log.info( "After initialization of system graph model component '%s' have version %d and status %s",
                COMPONENT, componentAfterInit.version, componentAfterInit.getStatus() );
    }

    @Override
    public void initializeSystemGraphConstraints( Transaction tx )
    {
        initializeSystemGraphConstraint( tx, USER_LABEL, "name" );
    }

    private void initializeLatestSystemGraph( Transaction tx ) throws Exception
    {
        KnownCommunitySecurityComponentVersion latest = knownUserSecurityComponentVersions.latestSecurityGraphVersion();
        log.debug( "Latest version of component '%s' is %s", COMPONENT, latest.version );
        latest.setupUsers( tx );
        latest.setVersionProperty( tx, latest.version );
    }

    @Override
    protected void postInitialization( GraphDatabaseService system, boolean wasInitialized ) throws Exception
    {
        try ( Transaction tx = system.beginTx() )
        {
            KnownCommunitySecurityComponentVersion component = knownUserSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx );
            log.info( "Performing postInitialization step for component '%s' with version %d and status %s",
                COMPONENT, component.version, component.getStatus() );

            // Do not need to setup initial password when initialized, because that is already done by the initialization code in `setupUsers`
            if ( !wasInitialized )
            {

                 log.info( "Updating the initial password in component '%s'  ", COMPONENT, component.version, component.getStatus() );
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
    public Optional<Exception> upgradeToCurrent( GraphDatabaseService system )
    {
        return SystemGraphComponent.executeWithFullAccess( system, tx ->
        {
            KnownCommunitySecurityComponentVersion currentVersion = knownUserSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx );
            log.debug( "Trying to upgrade component '%s' with version %d and status %s to latest version",
                    COMPONENT, currentVersion.version, currentVersion.getStatus() );
            if ( currentVersion.version == NoUserSecurityGraph.VERSION )
            {
                log.debug( "The current version does not have a security graph, doing a full initialization" );
                initializeLatestSystemGraph( tx );
            }
            else
            {
                if ( currentVersion.migrationSupported() )
                {
                    log.info( "Upgrading security graph to latest version" );
                    currentVersion.upgradeSecurityGraph( tx, knownUserSecurityComponentVersions.latestSecurityGraphVersion() );
                }
                else
                {
                    throw currentVersion.unsupported();
                }
            }
        } );
    }
}
