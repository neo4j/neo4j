package org.neo4j.server.rrd;

import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ImpermanentGraphDatabase;

import javax.management.MalformedObjectNameException;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class NodeIdsInUseSampleableTest
{
    @Test
    public void emptyDbHasZeroNodesInUse() throws IOException, MalformedObjectNameException
    {
        ImpermanentGraphDatabase db = new ImpermanentGraphDatabase();
        NodeIdsInUseSampleable sampleable = new NodeIdsInUseSampleable( db );

        assertThat( sampleable.getValue(), is( 1L ) ); //Reference node is always created in empty dbs
    }

    @Test
    public void addANodeAndSampleableGoesUp() throws IOException, MalformedObjectNameException
    {
        ImpermanentGraphDatabase db = new ImpermanentGraphDatabase();
        NodeIdsInUseSampleable sampleable = new NodeIdsInUseSampleable( db );
        createNode( db );

        assertThat( sampleable.getValue(), is( 2L ) ); 
    }

    private void createNode( ImpermanentGraphDatabase db )
    {
        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();
    }
}
