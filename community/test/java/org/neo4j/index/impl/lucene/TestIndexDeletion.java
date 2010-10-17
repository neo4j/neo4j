/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.index.impl.lucene;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.index.Neo4jTestCase.assertCollection;
import static org.neo4j.index.impl.lucene.Contains.contains;
import static org.neo4j.index.impl.lucene.HasThrownException.hasThrownException;


public class TestIndexDeletion
{
    private static final String INDEX_NAME = "index";
    private static GraphDatabaseService graphDb;
    private static LuceneIndexProvider provider;
    private Index<Node> index;
    private Transaction tx;
    private String key;
    private Node node;
    private String value;
    private List<WorkThread> workers;

    @BeforeClass
    public static void setUpStuff()
    {
        String storeDir = "target/var/freshindex";
        Neo4jTestCase.deleteFileOrDirectory( new File( storeDir ) );
        graphDb = new EmbeddedGraphDatabase( storeDir );
        provider = new LuceneIndexProvider( graphDb );
    }

    @AfterClass
    public static void tearDownStuff()
    {
        graphDb.shutdown();
    }

    @After
    public void commitTx()
    {
        finishTx( true );
        for ( WorkThread worker : workers )
        {
            worker.rollback();
            worker.die();
        }
    }

    public void rollbackTx()
    {
        finishTx( false );
    }

    public void finishTx( boolean success )
    {
        if ( tx != null )
        {
            if ( success )
            {
                tx.success();
            }
            tx.finish();
            tx = null;
        }
    }

    @Before
    public void createInitialData()
    {
        beginTx();
        index = provider.nodeIndex( INDEX_NAME, LuceneIndexProvider.EXACT_CONFIG );
        index.delete();
        restartTx();

        index = provider.nodeIndex( INDEX_NAME, LuceneIndexProvider.EXACT_CONFIG );
        key = "key";

        value = "my own value";
        node = graphDb.createNode();
        index.add( node, key, value );
        workers = new ArrayList<WorkThread>();
    }

    public void beginTx()
    {
        if ( tx == null )
        {
            tx = graphDb.beginTx();
        }
    }

    void restartTx()
    {
        finishTx( true );
        beginTx();
    }

    @Test
    public void shouldBeAbleToDeleteAndRecreateIndex()
    {
        restartTx();
        assertCollection( index.query( key, "own" ) );
        index.delete();
        restartTx();

        Index<Node> recreatedIndex = provider.nodeIndex( INDEX_NAME, LuceneIndexProvider.FULLTEXT_CONFIG );
        assertNull( recreatedIndex.get( key, value ).getSingle() );
        recreatedIndex.add( node, key, value );
        restartTx();
        assertCollection( recreatedIndex.query( key, "own" ), node );
        recreatedIndex.delete();
    }

    @Test
    public void shouldNotBeDeletedWhenDeletionRolledBack()
    {
        restartTx();
        index.delete();
        rollbackTx();
        index.get( key, value );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIllegalStateForActionsAfterDeletedOnIndex()
    {
        restartTx();
        index.delete();
        restartTx();
        index.query( key, "own" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIllegalStateForActionsAfterDeletedOnIndex2()
    {
        restartTx();
        index.delete();
        restartTx();
        index.add( node, key, value );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIllegalStateForActionsAfterDeletedOnIndex3()
    {
        restartTx();
        index.delete();
        index.query( key, "own" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIllegalStateForActionsAfterDeletedOnIndex4()
    {
        restartTx();
        index.delete();
        Index<Node> newIndex = provider.nodeIndex( INDEX_NAME, LuceneIndexProvider.EXACT_CONFIG );
        newIndex.query( key, "own" );
    }

    @Test
    public void deleteInOneTxShouldNotAffectTheOther() throws InterruptedException
    {
        index.delete();

        WorkThread firstTx = createWorker();
        firstTx.createNodeAndIndexBy( key, "another value" );
        firstTx.commit();
    }

    @Test
    public void deleteAndCommitShouldBePublishedToOtherTransaction2() throws InterruptedException
    {
        WorkThread firstTx = createWorker();
        WorkThread secondTx = createWorker();

        firstTx.beginTransaction();
        secondTx.beginTransaction();

        firstTx.createNodeAndIndexBy( key, "some value" );
        secondTx.createNodeAndIndexBy( key, "some other value" );

        firstTx.deleteIndex();
        firstTx.commit();

        secondTx.queryIndex( key, "some other value" );

        assertThat( secondTx, hasThrownException() );

        secondTx.rollback();
    }

    @Test
    public void indexDeletesShouldNotByVisibleUntilCommit()
    {
        commitTx();

        WorkThread firstTx = createWorker();
        WorkThread secondTx = createWorker();

        firstTx.beginTransaction();
        firstTx.removeFromIndex( key, value );

        assertThat( secondTx.queryIndex( key, value ), contains( node ) );

        firstTx.rollback();
    }

    private WorkThread createWorker()
    {
        WorkThread workThread = new WorkThread( index, graphDb );
        workers.add( workThread );
        return workThread;
    }
}
