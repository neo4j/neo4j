/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.batchinsert;

import static java.lang.Math.pow;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.AbstractNeo4jTestCase.deleteFileOrDirectory;
import static org.neo4j.kernel.impl.core.BigStoreIT.machineIsOkToRunThisTest;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.IdType;

public class BigBatchStoreIT implements RelationshipType
{
    private static final String PATH = "target/var/bigb";
    private BatchInserter db;
    public @Rule
    TestName testName = new TestName()
    {
        @Override
        public String getMethodName()
        {
            return BigBatchStoreIT.this.getClass().getSimpleName() + "#" + super.getMethodName();
        }
    };
    
    @Before
    public void doBefore()
    {
        // Delete before just to be sure
        deleteFileOrDirectory( new File( PATH ) );
        db = new BatchInserterImpl( PATH );
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
        testHighIds( (long) pow( 2, 32 ), 2, 1000 );
    }
    
    @Test
    public void create8BPlusStuff() throws Exception
    {
        testHighIds( (long) pow( 2, 33 ), 1, 1600 );
    }
    
    private void testHighIds( long highMark, int minus, int requiredHeapMb )
    {
        assumeTrue( machineIsOkToRunThisTest( testName.getMethodName(), requiredHeapMb ) );
        
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
        
        long nodeBelowTheLine = db.createNode( map( propertyKey, intPropertyValue ) );
        assertEquals( idBelow, nodeBelowTheLine );
        long nodeAboveTheLine = db.createNode( map( propertyKey, stringPropertyValue ) );
        long relBelowTheLine = db.createRelationship( nodeBelowTheLine, nodeAboveTheLine, this, map( propertyKey, arrayPropertyValue ) );
        assertEquals( idBelow, relBelowTheLine );
        long relAboveTheLine = db.createRelationship( nodeAboveTheLine, nodeBelowTheLine, this, null );
        assertEquals( highMark, relAboveTheLine );
        assertEquals( highMark, nodeAboveTheLine );
        assertEquals( intPropertyValue, db.getNodeProperties( nodeBelowTheLine ).get( propertyKey ) );
        assertEquals( stringPropertyValue, db.getNodeProperties( nodeAboveTheLine ).get( propertyKey ) );
        assertTrue( Arrays.equals( arrayPropertyValue, (long[]) db.getRelationshipProperties( relBelowTheLine ).get( propertyKey ) ) );

        assertEquals( asSet( asList( relBelowTheLine, relAboveTheLine ) ), asIds( db.getRelationships( idBelow ) ) );
        db.shutdown();
        db = new BatchInserterImpl( PATH );
        assertEquals( asSet( asList( relBelowTheLine, relAboveTheLine ) ), asIds( db.getRelationships( idBelow ) ) );
        db.shutdown();
        
        GraphDatabaseService edb = new EmbeddedGraphDatabase( PATH );
        assertEquals( nodeAboveTheLine, edb.getNodeById( highMark ).getId() );
        assertEquals( relBelowTheLine, edb.getNodeById( idBelow ).getSingleRelationship( this, Direction.OUTGOING ).getId() );
        assertEquals( relAboveTheLine, edb.getNodeById( idBelow ).getSingleRelationship( this, Direction.INCOMING ).getId() );
        assertEquals(   asSet( asList( edb.getRelationshipById( relBelowTheLine ), edb.getRelationshipById( relAboveTheLine ) ) ),
                        asSet( asCollection( edb.getNodeById( idBelow ).getRelationships() ) ) );
        edb.shutdown();
        db = new BatchInserterImpl( PATH );
    }
    
    @Test( expected=IllegalArgumentException.class )
    public void makeSureCantCreateNodeWithMagicNumber()
    {
        long id = (long) Math.pow( 2, 32 )-1;
        db.createNode( id, null );
    }
    
    private Collection<Long> asIds( Iterable<SimpleRelationship> relationships )
    {
        Collection<Long> ids = new HashSet<Long>();
        for ( SimpleRelationship rel : relationships )
        {
            ids.add( rel.getId() );
        }
        return ids;
    }

    private static <T> Collection<T> asSet( Collection<T> collection )
    {
        return new HashSet<T>( collection );
    }

    private void setHighId( IdType type, long highId )
    {
        ((BatchInserterImpl) db).getIdGeneratorFactory().get( type ).setHighId( highId );
    }
}
