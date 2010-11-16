package org.neo4j.server.rrd;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ImpermanentGraphDatabase;

import javax.management.MalformedObjectNameException;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class PropertyCountSampleableTest
{
    public ImpermanentGraphDatabase db;
    public PropertyCountSampleable sampleable;

    @Test
    public void emptyDbHasZeroNodesInUse() throws IOException, MalformedObjectNameException
    {
        assertThat( sampleable.getValue(), is( 0L ) ); 
    }

    @Test
    public void addANodeAndSampleableGoesUp() throws IOException, MalformedObjectNameException
    {
        addPropertyToReferenceNode( );

        assertThat( sampleable.getValue(), is( 1L ) );
    }

    private void addPropertyToReferenceNode( )
    {
        Transaction tx = db.beginTx();
        Node n = db.getReferenceNode();
        n.setProperty( "monkey", "rock!" );
        tx.success();
        tx.finish();
    }

    @Before
    public void setUp() throws Exception
    {
        db = new ImpermanentGraphDatabase();
        sampleable = new PropertyCountSampleable( db );
    }
}
