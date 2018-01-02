/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * This class logs whenever important cluster or high availability events
 * are issued.
 */
public class HighAvailabilityLogger
        implements ClusterMemberListener, ClusterListener, AvailabilityGuard.AvailabilityListener
{
    private final Log log;
    private final InstanceId myId;
    private URI myUri;

    public HighAvailabilityLogger( LogProvider logProvider, InstanceId myId )
    {
        this.log = logProvider.getLog( getClass() );
        this.myId = myId;
    }

    // Cluster events

    /**
     * Logged when the instance itself joins or rejoins a cluster
     *
     * @param clusterConfiguration
     */
    @Override
    public void enteredCluster( ClusterConfiguration clusterConfiguration )
    {
        myUri = clusterConfiguration.getUriForId( myId );
        log.info( "Instance %s joined the cluster", printId( myId, myUri ) );
    }

    /**
     * Logged when the instance itself leaves the cluster
     */
    @Override
    public void leftCluster()
    {
        log.info( "Instance %s left the cluster", printId( myId, myUri ) );
    }

    /**
     * Logged when another instance joins the cluster
     *
     * @param instanceId
     * @param member
     */
    @Override
    public void joinedCluster( InstanceId instanceId, URI member )
    {
        log.info( "Instance %s joined the cluster", printId( instanceId, member ) );
    }

    /**
     * Logged when another instance leaves the cluster
     *
     * @param instanceId
     */
    @Override
    public void leftCluster( InstanceId instanceId, URI member )
    {
        log.info( "Instance %s has left the cluster", printId( instanceId, member ) );
    }

    /**
     * Logged when an instance is elected for a role, such as coordinator of a cluster.
     *
     * @param role
     * @param instanceId
     * @param electedMember
     */
    @Override
    public void elected( String role, InstanceId instanceId, URI electedMember )
    {
        log.info( "Instance %s was elected as %s", printId( instanceId, electedMember ), role );
    }

    /**
     * Logged when an instance is demoted from a role.
     *
     * @param role
     * @param instanceId
     * @param electedMember
     */
    @Override
    public void unelected( String role, InstanceId instanceId, URI electedMember )
    {
        log.info( "Instance %s was demoted as %s", printId( instanceId, electedMember ), role );
    }

    // HA events
    @Override
    public void coordinatorIsElected( InstanceId coordinatorId )
    {
    }

    /**
     * Logged when a member becomes available as a role, such as MASTER or SLAVE.
     *
     * @param role
     * @param availableId the role connection information for the new role holder
     * @param atUri       the URI at which the instance is available at
     */
    @Override
    public void memberIsAvailable( String role, InstanceId availableId, URI atUri, StoreId storeId )
    {
        log.info( "Instance %s is available as %s at %s with %s", printId( availableId, atUri ), role, atUri.toASCIIString(), storeId );
    }

    /**
     * Logged when a member becomes unavailable as a role, such as MASTER or SLAVE.
     *
     * @param role          The role for which the member is unavailable
     * @param unavailableId The id of the member which became unavailable for that role
     */
    @Override
    public void memberIsUnavailable( String role, InstanceId unavailableId )
    {
        log.info( "Instance %s is unavailable as %s", printId( unavailableId, null ), role );
    }

    /**
     * Logged when another instance is detected as being failed.
     *
     * @param instanceId
     */
    @Override
    public void memberIsFailed( InstanceId instanceId )
    {
        log.info( "Instance %s has failed", printId( instanceId, null ) );
    }

    /**
     * Logged when another instance is detected as being alive again.
     *
     * @param instanceId
     */
    @Override
    public void memberIsAlive( InstanceId instanceId )
    {
        log.info( "Instance %s is alive", printId( instanceId, null ) );
    }

    // InstanceAccessGuard events

    /**
     * Logged when users are allowed to access the database for transactions.
     */
    @Override
    public void available()
    {
        log.info( "Database available for write transactions" );
    }

    /**
     * Logged when users are not allowed to access the database for transactions.
     */
    @Override
    public void unavailable()
    {
        log.info( "Write transactions to database disabled" );
    }

    private String printId( InstanceId id, URI member )
    {
        String name = id.instanceNameFromURI( member );
        return name + (id.equals( myId ) ? " (this server) " : " ");
    }
}
