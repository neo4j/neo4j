/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package legacy;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneFulltextIndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Asserts that LuceneIndexService and friends play nice with
 * {@link GraphDatabaseService#index()}
 */
public class TestPlayNiceWithIntegrated
{
    private GraphDatabaseService db;
    private Transaction tx;
    
    @Before
    public void doBefore()
    {
        db = new EmbeddedGraphDatabase( "target/playground" );
    }
    
    @After
    public void doAfter()
    {
        if ( tx != null )
        {
            commitTx();
        }
        db.shutdown();
    }
    
    private void beginTx()
    {
        tx = db.beginTx();
    }
    
    private void commitTx()
    {
        tx.success();
        tx.finish();
        tx = null;
    }
    
    private void restartTx()
    {
        commitTx();
        beginTx();
    }
    
    @Test
    public void smokeTestIt() throws Exception
    {
        IndexService indexService = new LuceneIndexService( db );
        IndexService fulltextIndexService = new LuceneFulltextIndexService( db );
        Index<Node> aNodeIndex = db.index().forNodes( "a" ); // "a" as in a node index
        String key = "key";
        String value = "value";
        
        beginTx();
        Node node = db.createNode();
        indexService.index( node, key, value );
        fulltextIndexService.index( node, key, value );
        aNodeIndex.add( node, key, value );
        
        for ( int i = 0; i < 2; i++ )
        {   // Do the assertions first just before the commit, then after
            assertEquals( node, indexService.getSingleNode( key, value ) );
            assertEquals( node, fulltextIndexService.getSingleNode( key, value ) );
            assertEquals( node, aNodeIndex.get( key, value ).getSingle() );
            restartTx();
        }
        
        fulltextIndexService.shutdown();
        indexService.shutdown();
    }
}
