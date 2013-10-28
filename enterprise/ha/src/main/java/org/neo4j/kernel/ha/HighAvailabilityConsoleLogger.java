/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.kernel.logging.ConsoleLogger;

/**
 * This class logs to the console whenever important cluster or high availability events
 * are issued.
 */
public class HighAvailabilityConsoleLogger
    implements ClusterMemberListener, ClusterListener, InstanceAccessGuard.AccessListener
{
    private final ConsoleLogger console;
    private final InstanceId myId;

    public HighAvailabilityConsoleLogger( ConsoleLogger console, InstanceId myId )
    {
        this.console = console;
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
        console.log( String.format("Instance %d (this server) joined the cluster", myId.toIntegerIndex() ));
    }

    /**
     * Logged when the instance itself leaves the cluster
     */
    @Override
    public void leftCluster()
    {
        console.log(String.format("Instance %d (this server) left the cluster", myId.toIntegerIndex() ));
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
        console.log("Instance "+instanceId+" joined the cluster");
    }

    /**
     * Logged when another instance leaves the cluster
     *
     * @param instanceId
     */
    @Override
    public void leftCluster( InstanceId instanceId )
    {
        console.log("Instance "+instanceId+" has left the cluster");
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
        console.log("Instance "+printId(instanceId)+"was elected as "+role);
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
        console.log("Instance "+printId(instanceId)+"was demoted as "+role);
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
     * @param atUri the URI at which the instance is available at
     */
    @Override
    public void memberIsAvailable( String role, InstanceId availableId, URI atUri )
    {
        console.log("Instance "+printId(availableId)+"is available as "+role+" at "+atUri.toASCIIString());
    }

    /**
     * Logged when a member becomes unavailable as a role, such as MASTER or SLAVE.
     *
     * @param role The role for which the member is unavailable
     * @param unavailableId The id of the member which became unavailable for that role
     */
    @Override
    public void memberIsUnavailable( String role, InstanceId unavailableId )
    {
        console.log("Instance "+printId(unavailableId)+"is unavailable as "+role);
    }

    /**
     * Logged when another instance is detected as being failed.
     *
     * @param instanceId
     */
    @Override
    public void memberIsFailed( InstanceId instanceId )
    {
        console.log("Instance "+printId(instanceId)+"has failed");
    }

    /**
     * Logged when another instance is detected as being alive again.
     *
     * @param instanceId
     */
    @Override
    public void memberIsAlive( InstanceId instanceId )
    {
        console.log("Instance "+printId(instanceId)+"is alive");
    }

    // InstanceAccessGuard events

    /**
     * Logged when users are allowed to access the database for transactions.
     */
    @Override
    public void accessGranted()
    {
        console.log( "Database available for write transactions" );
    }

    /**
     * Logged when users are not allowed to access the database for transactions.
     */
    @Override
    public void accessDenied()
    {
        console.log( "Write transactions to database disabled" );
    }

    private String printId( InstanceId id )
    {
        return id+(id != null && id.equals(myId)?" (this server) ":" ");
    }
}
