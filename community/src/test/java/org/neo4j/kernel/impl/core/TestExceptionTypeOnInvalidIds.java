package org.neo4j.kernel.impl.core;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;

public class TestExceptionTypeOnInvalidIds
{
    private static final long SMALL_POSSITIVE_INTEGER = 5;
    private static final long SMALL_NEGATIVE_INTEGER = -5;
    private static final long BIG_POSSITIVE_INTEGER = Integer.MAX_VALUE;
    private static final long BIG_NEGATIVE_INTEGER = Integer.MIN_VALUE;
    private static final long SMALL_POSSITIVE_LONG = ( (long) Integer.MAX_VALUE ) + 1;
    private static final long SMALL_NEGATIVE_LONG = -( (long) Integer.MIN_VALUE ) - 1;
    private static final long BIG_POSSITIVE_LONG = Long.MAX_VALUE;
    private static final long BIG_NEGATIVE_LONG = Long.MIN_VALUE;
    private static GraphDatabaseService graphdb;
    private static GraphDatabaseService graphDbReadOnly;
    private Transaction tx;

    @BeforeClass
    public static void createDatabase()
    {
        graphdb = new EmbeddedGraphDatabase( "target/id_test" );
        graphDbReadOnly = new EmbeddedReadOnlyGraphDatabase( "target/id_test" );
    }

    @AfterClass
    public static void destroyDatabase()
    {
        graphDbReadOnly.shutdown();
        graphDbReadOnly = null;
        graphdb.shutdown();
        graphdb = null;
    }

    @Before
    public void startTransaction()
    {
        tx = graphdb.beginTx();
    }

    @After
    public void endTransaction()
    {
        tx.finish();
        tx = null;
    }

    /* behaves as expected */
    @Test( expected = NotFoundException.class )
    public void getNodeBySmallPossitiveInteger() throws Exception
    {
        getNodeById( SMALL_POSSITIVE_INTEGER );
        getNodeByIdReadOnly( SMALL_POSSITIVE_INTEGER );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getNodeBySmallNegativeInteger() throws Exception
    {
        getNodeById( SMALL_NEGATIVE_INTEGER );
        getNodeByIdReadOnly( SMALL_NEGATIVE_INTEGER );
    }

    /* behaves as expected */
    @Test( expected = NotFoundException.class )
    public void getNodeByBigPossitiveInteger() throws Exception
    {
        getNodeById( BIG_POSSITIVE_INTEGER );
        getNodeByIdReadOnly( BIG_POSSITIVE_INTEGER );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getNodeByBigNegativeInteger() throws Exception
    {
        getNodeById( BIG_NEGATIVE_INTEGER );
        getNodeByIdReadOnly( BIG_NEGATIVE_INTEGER );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getNodeBySmallPossitiveLong() throws Exception
    {
        getNodeById( SMALL_POSSITIVE_LONG );
        getNodeByIdReadOnly( SMALL_POSSITIVE_LONG );
    }

    /* behaves as expected */
    @Test( expected = NotFoundException.class )
    public void getNodeBySmallNegativeLong() throws Exception
    {
        getNodeById( SMALL_NEGATIVE_LONG );
        getNodeByIdReadOnly( SMALL_NEGATIVE_LONG );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getNodeByBigPossitiveLong() throws Exception
    {
        getNodeById( BIG_POSSITIVE_LONG );
        getNodeByIdReadOnly( BIG_POSSITIVE_LONG );
    }

    /* finds the node with id=0, since that what the id truncates to */
    @Test( expected = NotFoundException.class )
    public void getNodeByBigNegativeLong() throws Exception
    {
        getNodeById( BIG_NEGATIVE_LONG );
        getNodeByIdReadOnly( BIG_NEGATIVE_LONG );
    }

    /* behaves as expected */
    @Test( expected = NotFoundException.class )
    public void getRelationshipBySmallPossitiveInteger() throws Exception
    {
        getRelationshipById( SMALL_POSSITIVE_INTEGER );
        getRelationshipByIdReadOnly( SMALL_POSSITIVE_INTEGER );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getRelationshipBySmallNegativeInteger() throws Exception
    {
        getRelationshipById( SMALL_NEGATIVE_INTEGER );
        getRelationshipByIdReadOnly( SMALL_POSSITIVE_INTEGER );
    }

    /* behaves as expected */
    @Test( expected = NotFoundException.class )
    public void getRelationshipByBigPossitiveInteger() throws Exception
    {
        getRelationshipById( BIG_POSSITIVE_INTEGER );
        getRelationshipByIdReadOnly( BIG_POSSITIVE_INTEGER );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getRelationshipByBigNegativeInteger() throws Exception
    {
        getRelationshipById( BIG_NEGATIVE_INTEGER );
        getRelationshipByIdReadOnly( BIG_NEGATIVE_INTEGER );        
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getRelationshipBySmallPossitiveLong() throws Exception
    {
        getRelationshipById( SMALL_POSSITIVE_LONG );
        getRelationshipByIdReadOnly( SMALL_POSSITIVE_LONG );
    }

    /* behaves as expected */
    @Test( expected = NotFoundException.class )
    public void getRelationshipBySmallNegativeLong() throws Exception
    {
        getRelationshipById( SMALL_NEGATIVE_LONG );
        getRelationshipByIdReadOnly( SMALL_NEGATIVE_LONG );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getRelationshipByBigPossitiveLong() throws Exception
    {
        getRelationshipById( BIG_POSSITIVE_LONG );
        getRelationshipByIdReadOnly( BIG_POSSITIVE_LONG );
    }

    /* behaves as expected */
    @Test( expected = NotFoundException.class )
    public void getRelationshipByBigNegativeLong() throws Exception
    {
        getRelationshipById( BIG_NEGATIVE_LONG );
        getRelationshipByIdReadOnly( BIG_NEGATIVE_LONG );
    }

    private void getNodeById( long index )
    {
        Node value = graphdb.getNodeById( index );
        fail( String.format(
                "Returned Node [0x%x] for index 0x%x (int value: 0x%x)",
                value.getId(), index, (int) index ) );
    }

    private void getNodeByIdReadOnly( long index )
    {
        Node value = graphDbReadOnly.getNodeById( index );
        fail( String.format(
                "Returned Node [0x%x] for index 0x%x (int value: 0x%x)",
                value.getId(), index, (int) index ) );
    }
    
    private void getRelationshipById( long index )
    {
        Relationship value = graphdb.getRelationshipById( index );
        fail( String.format(
                "Returned Relationship [0x%x] for index 0x%x (int value: 0x%x)",
                value.getId(), index, (int) index ) );
    }

    private void getRelationshipByIdReadOnly( long index )
    {
        Relationship value = graphDbReadOnly.getRelationshipById( index );
        fail( String.format(
                "Returned Relationship [0x%x] for index 0x%x (int value: 0x%x)",
                value.getId(), index, (int) index ) );
    }
}
