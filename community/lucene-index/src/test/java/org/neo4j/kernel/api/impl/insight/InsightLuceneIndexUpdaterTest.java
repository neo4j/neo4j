package org.neo4j.kernel.api.impl.insight;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;

public class InsightLuceneIndexUpdaterTest
{
    @Rule
    public EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();
    @Rule
    public DatabaseRule dbRule = new ImpermanentDatabaseRule();

    private static final Label LABEL = Label.label( "label1" );

    @Test
    public void shouldFindNodeWithString() throws IOException
    {
        // given
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        InsightIndex insightIndex = new InsightIndex( fileSystemRule.get(), new int[]{1} );
        db.registerTransactionEventHandler( insightIndex.getUpdater() );

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 1; i++ )
            {
                Node node = db.createNode( LABEL );
                node.setProperty( "prop", "aaaaa" );
            }

            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            tx.success();
        }
        InsightIndexReader reader = insightIndex.getReader();

        assertEquals( 0, reader.query( "aaaaa" ).next() );

    }
}
