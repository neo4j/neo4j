package org.neo4j.server.rrd;

import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ImpermanentGraphDatabase;

import javax.management.MalformedObjectNameException;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RelationshipCountSampleableTest
{
    @Test
    public void emptyDbHasZeroRelationships() throws IOException, MalformedObjectNameException
    {
        ImpermanentGraphDatabase db = new ImpermanentGraphDatabase();
        RelationshipCountSampleable sampleable = new RelationshipCountSampleable( db );

        assertThat( sampleable.getValue(), is( 0L ) );
    }

    @Test
    public void addANodeAndSampleableGoesUp() throws IOException, MalformedObjectNameException
    {
        ImpermanentGraphDatabase db = new ImpermanentGraphDatabase();
        RelationshipCountSampleable sampleable = new RelationshipCountSampleable( db );
        createNode( db );

        assertThat( sampleable.getValue(), is( 1L ) );
    }

    private void createNode( ImpermanentGraphDatabase db )
    {
        Transaction tx = db.beginTx();
        Node node1 = db.createNode();
        Node node2 = db.createNode();
        node1.createRelationshipTo( node2, DynamicRelationshipType.withName( "friend" ) );
        tx.success();
        tx.finish();
    }
}
