package org.neo4j.server.rest.web;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.UniquenessFactory;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.database.Database;

public class TraversalPagerTest
{
    private static final int LIST_LENGTH = 100;
    private String databasePath;
    private Database database;
    private Node startNode;

    @Before
    public void clearDb() throws IOException
    {
        databasePath = ServerTestUtils.createTempDir()
                .getAbsolutePath();
        database = new Database( ServerTestUtils.EMBEDDED_GRAPH_DATABASE_FACTORY, databasePath );
        createLinkedList( LIST_LENGTH, database );
    }

    private void createLinkedList( int listLength, Database db )
    {
        Transaction tx = db.graph.beginTx();
        try
        {
            Node previous = null;
            for ( int i = 0; i < listLength; i++ )
            {
                Node current = db.graph.createNode();

                if ( previous != null )
                {
                    previous.createRelationshipTo( current, DynamicRelationshipType.withName( "NEXT" ) );
                }
                else
                {
                    startNode = current;
                }

                previous = current;
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    @After
    public void shutdownDatabase() throws IOException
    {
        database.shutdown();
        FileUtils.forceDelete( new File( databasePath ) );
    }

    @Test
    public void shouldPageThroughResultsForWhollyDivisiblePageSize()
    {
        Traverser myTraverser = simpleListTraverser();
        TraversalPager traversalPager = new TraversalPager( myTraverser, LIST_LENGTH / 10 );

        iterateThroughPagedTraverser( traversalPager );

        assertNull( traversalPager.next() );

    }

    @SuppressWarnings( "unused" )
    private void iterateThroughPagedTraverser( TraversalPager traversalPager )
    {
        for ( List<Path> paths : traversalPager ) {}
    }

    @Test
    public void shouldPageThroughResultsForNonWhollyDivisiblePageSize()
    {
        int awkwardPageSize = 7;
        Traverser myTraverser = simpleListTraverser();
        TraversalPager traversalPager = new TraversalPager( myTraverser, awkwardPageSize  );

        iterateThroughPagedTraverser( traversalPager );

        assertNull( traversalPager.next() );
    }
    
    private Traverser simpleListTraverser()
    {
        return Traversal.description()
                .expand( Traversal.expanderForTypes( DynamicRelationshipType.withName( "NEXT" ), Direction.OUTGOING ) )
                .depthFirst()
                .uniqueness( Uniqueness.NODE_GLOBAL )
                .traverse( startNode );
    }
}
