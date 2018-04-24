package org.neo4j.kernel.bloom;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.bloom.BloomIT.GET_NODE_KEYS;
import static org.neo4j.kernel.bloom.BloomIT.GET_REL_KEYS;
import static org.neo4j.kernel.bloom.BloomIT.NODES;
import static org.neo4j.kernel.bloom.BloomIT.RELS;
import static org.neo4j.kernel.bloom.BloomIT.SET_NODE_KEYS;
import static org.neo4j.kernel.bloom.BloomIT.SET_REL_KEYS;

public class BloomClusterIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 1 ).withTimeout( 1000, SECONDS );

    private Cluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
        for ( CoreClusterMember coreClusterMember : cluster.coreMembers() )
        {
            coreClusterMember.database().getDependencyResolver().resolveDependency( Procedures.class ).registerProcedure( BloomProcedures.class );
        }
        for ( ReadReplica readReplica : cluster.readReplicas() )
        {
            readReplica.database().getDependencyResolver().resolveDependency( Procedures.class ).registerProcedure( BloomProcedures.class );
        }
    }

    @Test
    public void shouldReplicateBloomIndexContentsWhenPopulating() throws Exception
    {
        // when
        final Node[] node1 = new Node[1];
        final Node[] node2 = new Node[1];
        final Relationship[] relationship = new Relationship[1];
        cluster.coreTx( ( db, tx ) ->
        {
            node1[0] = db.createNode();
            node1[0].setProperty( "prop", "This is a integration test." );
            node2[0] = db.createNode();
            node2[0].setProperty( "otherprop", "This is a related integration test" );
            relationship[0] = node1[0].createRelationshipTo( node2[0], RelationshipType.withName( "type" ) );
            relationship[0].setProperty( "prop", "They relate" );
            tx.success();
        } );
        cluster.coreTx( ( db, tx ) ->
        {
            db.execute( String.format( SET_NODE_KEYS, "\"prop\", \"otherprop\"" ) );
            db.execute( String.format( SET_REL_KEYS, "\"prop\"" ) );
            tx.success();
        } );
        Thread.sleep( 2000 );

        // then
        query( asList( node1[0].getId(), node2[0].getId() ), String.format( NODES, "\"integration\"" ), "entityid", cluster );
        query( asList( node1[0].getId(), node2[0].getId() ), String.format( NODES, "\"test\"" ), "entityid", cluster );
        query( asList( node2[0].getId() ), String.format( NODES, "\"related\"" ), "entityid", cluster );
        query( asList( relationship[0].getId() ), String.format( RELS, "\"relate\"" ), "entityid", cluster );

    }

    @Test
    public void shouldReplicateBloomIndexExistenceToMembers() throws Exception
    {
        // when
        cluster.coreTx( ( db, tx ) ->
        {
            db.execute( String.format( SET_NODE_KEYS, "\"prop\", \"otherprop\", \"proppmatt\"" ) );
            db.execute( String.format( SET_REL_KEYS, "\"ata\", \"mata\", \"matt\"" ) );
            tx.success();
        } );
        Thread.sleep( 2000 );

        // then
        query( asList( "prop", "otherprop", "proppmatt" ), GET_NODE_KEYS, "propertyKey", cluster );
        query( asList( "ata", "mata", "matt" ), GET_REL_KEYS, "propertyKey", cluster );
    }

    public void query( List<Object> expected, String query, String key, Cluster cluster )
    {
        query( expected, query, key, cluster.coreMembers() );
        query( expected, query, key, cluster.readReplicas() );
    }

    public void query( List<Object> expected, String query, String key, Collection<? extends ClusterMember> clusterMembers )
    {
        for ( ClusterMember ClusterMember : clusterMembers )
        {
            Result result = ClusterMember.database().execute( query );
            Set<Object> results = new HashSet<>();
            while ( result.hasNext() )
            {
                results.add( result.next().get( key ) );
            }
            String errorMessage = errorMessage( results, expected );
            assertEquals( errorMessage, expected.size(), results.size() );
            int i = 0;
            while ( !results.isEmpty() )
            {
                assertTrue( errorMessage, results.remove( expected.get( i++ ) ) );
            }
        }
    }

    private String errorMessage( Set<Object> actual, List<Object> expected )
    {
        return String.format( "Query results differ from expected, expected %s but got %s", expected, actual );
    }
}
