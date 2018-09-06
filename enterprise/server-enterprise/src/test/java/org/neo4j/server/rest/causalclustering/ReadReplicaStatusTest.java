/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.rest.causalclustering;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.ws.rs.core.Response;

import org.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReadReplicaStatusTest
{
    private CausalClusteringStatus status;

    private FakeTopologyService topologyService;
    private Dependencies dependencyResolver = new Dependencies();
    private DatabaseHealth databaseHealth;
    private CommandIndexTracker commandIndexTracker;

    private final MemberId myself = new MemberId( UUID.randomUUID() );
    private final LogProvider logProvider = NullLogProvider.getInstance();

    @Before
    public void setup() throws Exception
    {
        OutputFormat output = new OutputFormat( new JsonFormat(), new URI( "http://base.local:1234/" ), null );
        ReadReplicaGraphDatabase db = mock( ReadReplicaGraphDatabase.class );
        topologyService = new FakeTopologyService( randomMembers( 3 ), randomMembers( 2 ), myself, RoleInfo.READ_REPLICA );
        dependencyResolver.satisfyDependencies( topologyService );

        when( db.getDependencyResolver() ).thenReturn( dependencyResolver );
        databaseHealth = dependencyResolver.satisfyDependency(
                new DatabaseHealth( mock( DatabasePanicEventGenerator.class ), logProvider.getLog( DatabaseHealth.class ) ) );
        commandIndexTracker = dependencyResolver.satisfyDependency( new CommandIndexTracker() );

        status = CausalClusteringStatusFactory.build( output, db );
    }

    @Test
    public void testAnswers()
    {
        // when
        Response available = status.available();
        Response readonly = status.readonly();
        Response writable = status.writable();

        // then
        assertEquals( OK.getStatusCode(), available.getStatus() );
        assertEquals( "true", available.getEntity() );

        assertEquals( OK.getStatusCode(), readonly.getStatus() );
        assertEquals( "true", readonly.getEntity() );

        assertEquals( NOT_FOUND.getStatusCode(), writable.getStatus() );
        assertEquals( "false", writable.getEntity() );
    }

    @Test
    public void statusIncludesAppliedRaftLogIndex() throws IOException
    {
        // given
        commandIndexTracker.setAppliedCommandIndex( 321 );

        // when
        Response description = status.description();

        // then
        Map<String,Object> responseJson = responseAsMap( description );
        assertEquals( 321, responseJson.get( "lastAppliedRaftIndex" ) );
    }

    @Test
    public void responseIncludesAllCoresAndReplicas() throws IOException
    {
        Response description = status.description();

        assertEquals( Response.Status.OK.getStatusCode(), description.getStatus() );
        ArrayList<String> expectedVotingMembers = topologyService.allCoreServers()
                .members()
                .keySet()
                .stream()
                .map( memberId -> memberId.getUuid().toString() )
                .collect( Collectors.toCollection( ArrayList::new ) );
        Map<String,Object> responseJson = responseAsMap( description );
        List<String> actualVotingMembers = (List<String>) responseJson.get( "votingMembers" );
        Collections.sort( expectedVotingMembers );
        Collections.sort( actualVotingMembers );
        assertEquals( expectedVotingMembers, actualVotingMembers );
    }

    @Test
    public void dbHealthIsIncludedInResponse() throws IOException
    {
        Response description = status.description();
        assertEquals( true, responseAsMap( description ).get( "healthy" ) );

        databaseHealth.panic( new RuntimeException() );
        description = status.description();
        assertEquals( false, responseAsMap( description ).get( "healthy" ) );
    }

    @Test
    public void includesMemberId() throws IOException
    {
        Response description = status.description();
        assertEquals( myself.getUuid().toString(), responseAsMap( description ).get( "memberId" ) );
    }

    @Test
    public void leaderIsOptional() throws IOException
    {
        Response description = status.description();
        assertFalse( responseAsMap( description ).containsKey( "leader" ) );

        MemberId selectedLead = topologyService.allCoreServers()
                .members()
                .keySet()
                .stream()
                .findFirst()
                .orElseThrow( () -> new IllegalStateException( "No cores in topology" ) );
        topologyService.replaceWithRole( selectedLead, RoleInfo.LEADER );
        description = status.description();
        assertEquals( selectedLead.getUuid().toString(), responseAsMap( description ).get( "leader" ) );
    }

    @Test
    public void isNotCore() throws IOException
    {
        Response description = status.description();
        assertTrue( responseAsMap( description ).containsKey( "core" ) );
        assertEquals( false, responseAsMap( status.description() ).get( "core" ) );
    }

    static Collection<MemberId> randomMembers( int size )
    {
        return IntStream.range( 0, size )
                .mapToObj( i -> UUID.randomUUID() )
                .map( MemberId::new )
                .collect( Collectors.toList());
    }

    static Map<String,Object> responseAsMap( Response response ) throws IOException
    {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String,Object> responseJson = objectMapper.readValue( response.getEntity().toString(), new TypeReference<Map<String,Object>>()
        {
        } );
        return responseJson;
    }
}
