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
package org.neo4j.server.security.systemgraph;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.AbstractSystemGraphComponent;
import org.neo4j.dbms.database.ComponentVersion;
import org.neo4j.dbms.database.KnownSystemComponentVersions;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.versions.CommunitySecurityComponentVersion_1_40;
import org.neo4j.server.security.systemgraph.versions.CommunitySecurityComponentVersion_2_41;
import org.neo4j.server.security.systemgraph.versions.CommunitySecurityComponentVersion_3_43D4;
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion;
import org.neo4j.server.security.systemgraph.versions.NoCommunitySecurityComponentVersion;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.dbms.database.ComponentVersion.SECURITY_USER_COMPONENT;
import static org.neo4j.dbms.database.KnownSystemComponentVersion.UNKNOWN_VERSION;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_LABEL;

/**
 * This component contains the users of the dbms.
 * Each user is represented by a node in the system database with the label :User and properties for username, credentials, passwordChangeRequired and status.
 * The schema is the same in both community and enterprise (even if status is an enterprise-only feature).
 */
public class UserSecurityGraphComponent extends AbstractSystemGraphComponent
{
    private final KnownSystemComponentVersions<KnownCommunitySecurityComponentVersion> knownUserSecurityComponentVersions =
            new KnownSystemComponentVersions<>( new NoCommunitySecurityComponentVersion() );
    private final AbstractSecurityLog securityLog;

    public UserSecurityGraphComponent( AbstractSecurityLog securityLog, UserRepository initialPasswordRepo, Config config )
    {
        super( config );
        this.securityLog = securityLog;
        KnownCommunitySecurityComponentVersion version1 = new CommunitySecurityComponentVersion_1_40( securityLog, initialPasswordRepo );
        KnownCommunitySecurityComponentVersion version2 = new CommunitySecurityComponentVersion_2_41( securityLog, initialPasswordRepo, version1 );
        KnownCommunitySecurityComponentVersion version3 = new CommunitySecurityComponentVersion_3_43D4( securityLog, initialPasswordRepo, version2 );

        knownUserSecurityComponentVersions.add( version1 );
        knownUserSecurityComponentVersions.add( version2 );
        knownUserSecurityComponentVersions.add( version3 );
    }

    @Override
    public String componentName()
    {
        return SECURITY_USER_COMPONENT;
    }

    @Override
    public Status detect( Transaction tx )
    {
        return knownUserSecurityComponentVersions.detectCurrentComponentVersion( tx ).getStatus();
    }

    @Override
    public void initializeSystemGraphModel( Transaction tx ) throws Exception
    {
        KnownCommunitySecurityComponentVersion componentBeforeInit = knownUserSecurityComponentVersions.detectCurrentComponentVersion( tx );
        securityLog.info( String.format( "Initializing system graph model for component '%s' with version %d and status %s",
                          SECURITY_USER_COMPONENT, componentBeforeInit.version, componentBeforeInit.getStatus() ) );
        initializeLatestSystemGraph( tx );
        KnownCommunitySecurityComponentVersion componentAfterInit = knownUserSecurityComponentVersions.detectCurrentComponentVersion( tx );
        securityLog.info( String.format( "After initialization of system graph model component '%s' have version %d and status %s",
                          SECURITY_USER_COMPONENT, componentAfterInit.version, componentAfterInit.getStatus() ) );
    }

    @Override
    public void initializeSystemGraphConstraints( Transaction tx )
    {
        initializeSystemGraphConstraint( tx, USER_LABEL, "name" );
    }

    private void initializeLatestSystemGraph( Transaction tx ) throws Exception
    {
        KnownCommunitySecurityComponentVersion latest = knownUserSecurityComponentVersions.latestComponentVersion();
        securityLog.debug( String.format( "Latest version of component '%s' is %s", SECURITY_USER_COMPONENT, latest.version ) );
        latest.setupUsers( tx );
        latest.setVersionProperty( tx, latest.version );
    }

    @VisibleForTesting
    @Override
    public void postInitialization( GraphDatabaseService system, boolean wasInitialized ) throws Exception
    {
        try ( Transaction tx = system.beginTx() )
        {
            KnownCommunitySecurityComponentVersion component = knownUserSecurityComponentVersions.detectCurrentComponentVersion( tx );
            securityLog.info( String.format( "Performing postInitialization step for component '%s' with version %d and status %s",
                              SECURITY_USER_COMPONENT, component.version, component.getStatus() ) );

            // Do not need to setup initial password when initialized, because that is already done by the initialization code in `setupUsers`
            if ( !wasInitialized )
            {

                securityLog.info( String.format( "Updating the initial password in component '%s'", SECURITY_USER_COMPONENT ) );
                component.updateInitialUserPassword( tx );
                tx.commit();
            }
        }
    }

    @Override
    public void upgradeToCurrent( GraphDatabaseService system ) throws Exception
    {
        SystemGraphComponent.executeWithFullAccess( system, tx ->
        {
            KnownCommunitySecurityComponentVersion currentVersion = knownUserSecurityComponentVersions.detectCurrentComponentVersion( tx );
            securityLog.debug( String.format( "Trying to upgrade component '%s' with version %d and status %s to latest version",
                               SECURITY_USER_COMPONENT, currentVersion.version, currentVersion.getStatus() ) );
            if ( currentVersion.version == UNKNOWN_VERSION )
            {
                securityLog.debug( "The current version does not have a security graph, doing a full initialization" );
                initializeLatestSystemGraph( tx );
            }
            else
            {
                if ( currentVersion.migrationSupported() )
                {
                    securityLog.info( "Upgrading security graph to latest version" );
                    knownUserSecurityComponentVersions.latestComponentVersion().upgradeSecurityGraph( tx, currentVersion.version );
                }
                else
                {
                    throw currentVersion.unsupported();
                }
            }
        } );
    }

    public KnownCommunitySecurityComponentVersion findSecurityGraphComponentVersion( ComponentVersion version )
    {
        return knownUserSecurityComponentVersions.findComponentVersion( version );
    }
}
