package org.neo4j.kernel.impl.core;

import static java.lang.Math.pow;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.kernel.impl.AbstractNeo4jTestCase.deleteFileOrDirectory;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.IdType;

@Ignore( "Causes OOM, and won't run very nicely on Windows" )
public class TestBigStore implements RelationshipType
{
    private static final String PATH = "target/var/big";
    private GraphDatabaseService db;
    
    @Before
    public void doBefore()
    {
        // Delete before just to be sure
        deleteFileOrDirectory( new File( PATH ) );
        db = new EmbeddedGraphDatabase( PATH );
    }
    
    @After
    public void doAfter()
    {
        db.shutdown();
        // Delete after because it's so darn big
        deleteFileOrDirectory( new File( PATH ) );
    }
    
    @Override
    public String name()
    {
        return "BIG_TYPE";
    }
    
    @Test
    public void create4BPlusStuff() throws Exception
    {
        testHighIds( (long) pow( 2, 32 ), 2 );
    }
    
    @Test
    public void create8BPlusStuff() throws Exception
    {
        testHighIds( (long) pow( 2, 33 ), 1 );
    }
    
    private void testHighIds( long highMark, int minus )
    {
        long idBelow = highMark-minus;
        setHighId( IdType.NODE, idBelow );
        setHighId( IdType.RELATIONSHIP, idBelow );
        setHighId( IdType.PROPERTY, idBelow );
        setHighId( IdType.ARRAY_BLOCK, idBelow );
        setHighId( IdType.STRING_BLOCK, idBelow );
        String propertyKey = "name";
        int intPropertyValue = 123;
        String stringPropertyValue = "Long string, longer than would fit in shortstring";
        long[] arrayPropertyValue = new long[] { 1021L, 321L, 343212L };
        
        Transaction tx = db.beginTx();
        Node nodeBelowTheLine = db.createNode();
        nodeBelowTheLine.setProperty( propertyKey, intPropertyValue );
        assertEquals( idBelow, nodeBelowTheLine.getId() );
        Node nodeAboveTheLine = db.createNode();
        nodeAboveTheLine.setProperty( propertyKey, stringPropertyValue );
        Relationship relBelowTheLine = nodeBelowTheLine.createRelationshipTo( nodeAboveTheLine, this );
        relBelowTheLine.setProperty( propertyKey, arrayPropertyValue );
        assertEquals( idBelow, relBelowTheLine.getId() );
        Relationship relAboveTheLine = nodeAboveTheLine.createRelationshipTo( nodeBelowTheLine, this );
        assertEquals( highMark, relAboveTheLine.getId() );
        assertEquals( highMark, nodeAboveTheLine.getId() );
        assertEquals( intPropertyValue, nodeBelowTheLine.getProperty( propertyKey ) );
        assertEquals( stringPropertyValue, nodeAboveTheLine.getProperty( propertyKey ) );
        assertTrue( Arrays.equals( arrayPropertyValue, (long[]) relBelowTheLine.getProperty( propertyKey ) ) );
        tx.success();
        tx.finish();

        for ( int i = 0; i < 2; i++ )
        {
            assertEquals( nodeAboveTheLine, db.getNodeById( highMark ) );
            assertEquals( idBelow, nodeBelowTheLine.getId() );
            assertEquals( highMark, nodeAboveTheLine.getId() );
            assertEquals( idBelow, relBelowTheLine.getId() );
            assertEquals( highMark, relAboveTheLine.getId() );
            assertEquals( relBelowTheLine, db.getNodeById( idBelow ).getSingleRelationship( this, Direction.OUTGOING ) );
            assertEquals( relAboveTheLine, db.getNodeById( idBelow ).getSingleRelationship( this, Direction.INCOMING ) );
            assertEquals( idBelow, relBelowTheLine.getId() );
            assertEquals( highMark, relAboveTheLine.getId() );
            assertEquals(   asSet( asList( relBelowTheLine, relAboveTheLine ) ),
                            asSet( asCollection( db.getNodeById( idBelow ).getRelationships() ) ) );
            
            if ( i == 0 )
            {
                System.out.println( "restarting" );
                db.shutdown();
                db = new EmbeddedGraphDatabase( PATH );
            }
        }
    }
    
    private static <T> Collection<T> asSet( Collection<T> collection )
    {
        return new HashSet<T>( collection );
    }

    private void setHighId( IdType type, long highId )
    {
        ((AbstractGraphDatabase) db).getConfig().getIdGeneratorFactory().get( type ).setHighId( highId );
    }
}
