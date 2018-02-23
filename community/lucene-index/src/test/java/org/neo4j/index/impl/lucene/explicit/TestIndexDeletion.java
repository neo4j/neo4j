/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.impl.lucene.explicit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.index.Neo4jTestCase.assertContains;
import static org.neo4j.index.impl.lucene.explicit.Contains.contains;
import static org.neo4j.index.impl.lucene.explicit.LuceneIndexImplementation.FULLTEXT_CONFIG;

class TestIndexDeletion
{
    private static final String INDEX_NAME = "index";
    private static GraphDatabaseService graphDb;
    private Index<Node> index;
    private Transaction tx;
    private String key;
    private Node node;
    private String value;
    private List<WorkThread> workers;

    @BeforeAll
    static void setUpStuff()
    {
        graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @AfterAll
    static void tearDownStuff()
    {
        graphDb.shutdown();
    }

    @AfterEach
    void commitTx() throws Exception
    {
        finishTx( true );
        for ( WorkThread worker : workers )
        {
            worker.rollback();
            worker.die();
            worker.close();
        }
    }

    private void rollbackTx()
    {
        finishTx( false );
    }

    private void finishTx( boolean success )
    {
        if ( tx != null )
        {
            if ( success )
            {
                tx.success();
            }
            tx.close();
            tx = null;
        }
    }

    @BeforeEach
    void createInitialData()
    {
        beginTx();
        index = graphDb.index().forNodes( INDEX_NAME );
        index.delete();
        restartTx();

        index = graphDb.index().forNodes( INDEX_NAME );
        key = "key";

        value = "my own value";
        node = graphDb.createNode();
        index.add( node, key, value );
        workers = new ArrayList<>();
    }

    private void beginTx()
    {
        if ( tx == null )
        {
            tx = graphDb.beginTx();
        }
    }

    private void restartTx()
    {
        finishTx( true );
        beginTx();
    }

    @Test
    void shouldBeAbleToDeleteAndRecreateIndex()
    {
        restartTx();
        assertContains( index.query( key, "own" ) );
        index.delete();
        restartTx();

        Index<Node> recreatedIndex = graphDb.index().forNodes( INDEX_NAME, FULLTEXT_CONFIG );
        assertNull( recreatedIndex.get( key, value ).getSingle() );
        recreatedIndex.add( node, key, value );
        restartTx();
        assertContains( recreatedIndex.query( key, "own" ), node );
        recreatedIndex.delete();
    }

    @Test
    void shouldNotBeDeletedWhenDeletionRolledBack()
    {
        restartTx();
        index.delete();
        rollbackTx();
        beginTx();
        try ( IndexHits<Node> indexHits = index.get( key, value ) )
        {
            //empty
        }
    }

    @Test
    void shouldThrowIllegalStateForActionsAfterDeletedOnIndex()
    {
        restartTx();
        index.delete();
        restartTx();
        try
        {
            index.query( key, "own" );
            fail( "Should fail" );
        }
        catch ( NotFoundException e )
        {
            assertThat( e.getMessage(), containsString( "doesn't exist" ) );
        }
    }

    @Test
    void shouldThrowIllegalStateForActionsAfterDeletedOnIndex2()
    {
        restartTx();
        index.delete();
        restartTx();
        try
        {
            index.add( node, key, value );
            fail( "Failure was expected" );
        }
        catch ( NotFoundException e )
        {
            assertThat( e.getMessage(), containsString( "doesn't exist" ) );
        }
    }

    @Test
    void shouldThrowIllegalStateForActionsAfterDeletedOnIndex3()
    {
        assertThrows( IllegalStateException.class, () -> {
            restartTx();
            index.delete();
            index.query( key, "own" );
        } );
    }

    @Test
    void shouldThrowIllegalStateForActionsAfterDeletedOnIndex4()
    {
        assertThrows( IllegalStateException.class, () -> {
            restartTx();
            index.delete();
            Index<Node> newIndex = graphDb.index().forNodes( INDEX_NAME );
            newIndex.query( key, "own" );
        } );
    }

    @Test
    void deleteInOneTxShouldNotAffectTheOther() throws Exception
    {
        index.delete();

        WorkThread firstTx = createWorker( "Single" );
        firstTx.beginTransaction();
        firstTx.createNodeAndIndexBy( key, "another value" );
        firstTx.commit();
    }

    @Test
    void deleteAndCommitShouldBePublishedToOtherTransaction2() throws Exception
    {
        WorkThread firstTx = createWorker( "First" );
        WorkThread secondTx = createWorker( "Second" );

        firstTx.beginTransaction();
        secondTx.beginTransaction();

        firstTx.createNodeAndIndexBy( key, "some value" );
        secondTx.createNodeAndIndexBy( key, "some other value" );

        firstTx.deleteIndex();
        firstTx.commit();

        try
        {
            secondTx.queryIndex( key, "some other value" );
            fail( "Should throw exception" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), instanceOf( NotFoundException.class ) );
            assertThat( e.getCause().getMessage().toLowerCase(), containsString( "index 'index' doesn't exist" ) );
        }

        secondTx.rollback();

        // Since $Before will start a tx, add a value and keep tx open and
        // workers will delete the index so this test will fail in @AfterEach
        // if we don't rollback this tx
        rollbackTx();
    }

    @Test
    void indexDeletesShouldNotByVisibleUntilCommit() throws Exception
    {
        commitTx();

        WorkThread firstTx = createWorker( "First" );

        firstTx.beginTransaction();
        firstTx.removeFromIndex( key, value );

        try ( Transaction transaction = graphDb.beginTx() )
        {
            IndexHits<Node> indexHits = index.get( key, value );
            assertThat( indexHits, contains( node ) );
        }

        firstTx.rollback();
    }

    @Test
    void canDeleteIndexEvenIfEntitiesAreFoundToBeAbandonedInTheSameTx()
    {
        // create and index a node
        Index<Node> nodeIndex = graphDb.index().forNodes( "index" );
        Node node = graphDb.createNode();
        nodeIndex.add( node, "key", "value" );
        // make sure to commit the creation of the entry
        restartTx();

        // delete the node to abandon the index entry
        node.delete();
        restartTx();

        // iterate over all nodes indexed with the key to discover abandoned
        for ( Node ignore : nodeIndex.get( "key", "value" ) )
        {
        }

        nodeIndex.delete();
        restartTx();
    }

    private WorkThread createWorker( String name )
    {
        WorkThread workThread = new WorkThread( name, index, graphDb, node );
        workers.add( workThread );
        return workThread;
    }
}
