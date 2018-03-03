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
package org.neo4j.causalclustering.scenarios;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.procedures.InstalledProtocolsProcedure;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.neo4j.causalclustering.protocol.Protocol.Identifier.RAFT;
import static org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation.Client.OUTBOUND;
import static org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation.Server.INBOUND;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class InstalledProtocolsProcedureIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withSharedCoreParam( CausalClusteringSettings.leader_election_timeout, "2s" )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 );
    private Cluster cluster;
    private CoreClusterMember leader;

    @Before
    public void startUp() throws Exception
    {
        cluster = clusterRule.startCluster();
        leader = cluster.awaitLeader();
    }

    @Test
    public void shouldSeeOutboundInstalledProtocolsOnLeader() throws Throwable
    {
        ProtocolInfo[] expectedProtocolInfos = cluster.coreMembers()
                .stream()
                .filter( member -> !member.equals( leader ) )
                .map( member -> new ProtocolInfo( OUTBOUND, localhost( member.raftListenAddress() ), RAFT.canonicalName(), 1 ) )
                .toArray( ProtocolInfo[]::new );

        assertEventually( "should see outbound installed protocols on core " + leader.serverId(),
                () -> installedProtocols( leader.database(), OUTBOUND ),
                hasItems( expectedProtocolInfos ),
                60, SECONDS );
    }

    @Test
    public void shouldSeeInboundInstalledProtocolsOnLeader() throws Throwable
    {
        assertEventually( "should see inbound installed protocols on core " + leader.serverId(),
                () -> installedProtocols( leader.database(), INBOUND ),
                hasSize( greaterThanOrEqualTo( cluster.coreMembers().size() - 1 ) ),
                60, SECONDS );
    }

    private List<ProtocolInfo> installedProtocols( GraphDatabaseFacade db, String wantedOrientation )
            throws TransactionFailureException, ProcedureException
    {
        InwardKernel kernel = db.getDependencyResolver().resolveDependency( InwardKernel.class );
        KernelTransaction transaction = kernel.newTransaction( Transaction.Type.implicit, AnonymousContext.read() );
        List<ProtocolInfo> infos = new LinkedList<>();
        try ( Statement statement = transaction.acquireStatement() )
        {
            RawIterator<Object[],ProcedureException> itr = statement.procedureCallOperations().procedureCallRead(
                    procedureName( "dbms", "cluster", InstalledProtocolsProcedure.PROCEDURE_NAME ), null );

            while ( itr.hasNext() )
            {
                Object[] row = itr.next();
                String orientation = (String) row[0];
                String address = localhost( (String) row[1] );
                String protocol = (String) row[2];
                long version = (long) row[3];
                if ( orientation.equals( wantedOrientation ) )
                {
                    infos.add( new ProtocolInfo( orientation, address, protocol, version ) );
                }
            }
        }
        return infos;
    }

    private String localhost( String uri )
    {
        return uri.replace( "127.0.0.1", "localhost" );
    }

    private static class ProtocolInfo
    {
        private final String orientation;
        private final String address;
        private final String protocol;
        private final long version;

        private ProtocolInfo( String orientation, String address, String protocol, long version )
        {
            this.orientation = orientation;
            this.address = address;
            this.protocol = protocol;
            this.version = version;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            ProtocolInfo that = (ProtocolInfo) o;
            return version == that.version && Objects.equals( orientation, that.orientation ) && Objects.equals( address, that.address ) &&
                    Objects.equals( protocol, that.protocol );
        }

        @Override
        public int hashCode()
        {

            return Objects.hash( orientation, address, protocol, version );
        }

        @Override
        public String toString()
        {
            return "ProtocolInfo{" + "orientation='" + orientation + '\'' + ", address='" + address + '\'' + ", protocol='" + protocol + '\'' + ", version=" +
                    version + '}';
        }
    }
}
