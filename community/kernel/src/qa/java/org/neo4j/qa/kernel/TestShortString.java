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
package org.neo4j.qa.kernel;

import java.io.File;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.MapUtil.*;

public class TestShortString
{
    private static final String DEFAULT_SHORT_STRING = "test";
    
    private static final String PATH = "target/var/qa-shortstring";
    private GraphDatabaseService db;
    
    @Before
    public void doBefore()
    {
        AbstractNeo4jTestCase.deleteFileOrDirectory( new File( PATH ) );
        db = new GraphDatabaseFactory().newEmbeddedDatabase( PATH );
    }
    
    @After
    public void doAfter()
    {
        db.shutdown();
    }
    
    @Test
    public void justToMakeSureThisQaTestSuiteHoldsWater() throws Exception
    {
        String key = "type";
        String aLongString = "This string should definately not fit in a shortstring";
        long sizeBefore = getSizeOfStringStore();
        createNode( map( key, aLongString ) );
        long sizeAfter = getSizeOfStringStore();
        assertTrue( sizeAfter > sizeBefore );
    }
    
    @Test
    public void makeSureNoDynamicRecordIsCreatedForShortStringOnEmptyStore() throws Exception
    {
        makeSureAShortStringWontGrowStringStore( DEFAULT_SHORT_STRING );
    }

    @Test
    public void makeSureNoDynamicRecordIsCreatedForShortStringOnExistingData() throws Exception
    {
        createNode( map( "name", "Neo" ) );
        createNode( map( "name", "A longer name, not fit for shortstring" ) );
        
        makeSureAShortStringWontGrowStringStore( DEFAULT_SHORT_STRING );
    }
    
    @Test
    public void makeSureSomeDynamicRecordIsCreatedForStringOnExistingData() throws Exception
    {
        createNode( map( "name", "Neo" ) );
        createNode( map( "name", "A longer name, not fit for shortstring" ) );
        
        makeSureAShortStringWillGrowStringStore( "$1\"üedsa" );
    }
    
    @Test
    public void makeSureNumericalShortStringJustBelowLimitWontGrowStringStore() throws Exception
    {
        makeSureAShortStringWontGrowStringStore( "+123456789 1234" );
    }
    
    @Test
    public void makeSureNumericalShortStringJustAboveLimitGrowStringStore() throws Exception
    {
        makeSureAShortStringWillGrowStringStore( "+123456789 01234" );
    }
    
    @Test
    public void makeSureCaseShortStringJustBelowLimitWontGrowStringStore() throws Exception
    {
        makeSureAShortStringWontGrowStringStore( "SOME_VALUE:T" );
    }
    
    @Test
    public void makeSureCaseShortStringJustAboveLimitGrowStringStore() throws Exception
    {
        makeSureAShortStringWillGrowStringStore( "SOMETHING_LON" );
    }
    
    @Test
    public void makeSureAlphaNumericalShortStringJustBelowLimitWontGrowStringStore() throws Exception
    {
        makeSureAShortStringWontGrowStringStore( "Mattias Yu" );
    }
    
    @Test
    public void makeSureAlphaNumericalShortStringJustAboveLimitGrowStringStore() throws Exception
    {
        makeSureAShortStringWillGrowStringStore( "Mattias Yup" );
    }
    
    @Test
    public void makeSureUtf8ShortStringJustBelowLimitWontGrowStringStore() throws Exception
    {
        makeSureAShortStringWontGrowStringStore( "¡@$#abc" );
    }
    
    @Test
    public void makeSureUtf8ShortStringJustAboveLimitGrowStringStore() throws Exception
    {
        makeSureAShortStringWillGrowStringStore( "¡@$#abcd" );
    }
    
    @Test
    public void makeSureRemoveShortStringWontGrowStringStoreFromShortStringDb() throws Exception
    {
        createNode( map( "key1", "one", "key2", "two" ) );
        
        long node = createNode( map( "name", "Neo" ) );
        long sizeBefore = getSizeOfStringStore();
        removeProperty( node, "name" );
        assertEquals( sizeBefore, getSizeOfStringStore() );
    }
    
    @Test
    public void makeSureRemoveShortStringWontGrowStringStoreFromMixedDb() throws Exception
    {
        createNode( map( "key1", "one", "key2", "A string not fit for shortstring" ) );
        
        long node = createNode( map( "name", "Neo" ) );
        long sizeBefore = getSizeOfStringStore();
        removeProperty( node, "name" );
        assertEquals( sizeBefore, getSizeOfStringStore() );
    }
    
    @Test
    public void makeSureUpdateShortStringWontGrowStringStoreFromShortStringDb() throws Exception
    {
        createNode( map( "key1", "one", "key2", "two" ) );
        
        long node = createNode( map( "name", "Neo" ) );
        long sizeBefore = getSizeOfStringStore();
        setProperty( node, "name", "new value" );
        assertEquals( sizeBefore, getSizeOfStringStore() );
    }
    
    @Test
    public void makeSureUpdateShortStringWontGrowStringStoreFromMixedDb() throws Exception
    {
        createNode( map( "key1", "one", "key2", "A string not fit for shortstring" ) );
        
        long node = createNode( map( "name", "Neo" ) );
        long sizeBefore = getSizeOfStringStore();
        setProperty( node, "name", "new value" );
        assertEquals( sizeBefore, getSizeOfStringStore() );
    }
    
    private void removeProperty( long node, String key )
    {
        Transaction tx = db.beginTx();
        db.getNodeById( node ).removeProperty( key );
        tx.success();
        tx.finish();
    }

    private void setProperty( long node, String key, Object value )
    {
        Transaction tx = db.beginTx();
        db.getNodeById( node ).setProperty( key, value );
        tx.success();
        tx.finish();
    }
    
    private void makeSureAShortStringWontGrowStringStore( String shortString )
    {
        assertEquals( 0, stringStoreDiff( shortString ) );
    }
    
    private void makeSureAShortStringWillGrowStringStore( String shortString )
    {
        assertTrue( stringStoreDiff( shortString ) > 0 );
    }
    
    private long stringStoreDiff( String propertyValue )
    {
        String key = "type";
        long sizeBefore = getSizeOfStringStore();
        long node = createNode( map( key, propertyValue ) );
        long sizeAfter = getSizeOfStringStore();
        assertEquals( propertyValue, db.getNodeById( node ).getProperty( key ) );
        return sizeAfter - sizeBefore;
    }
    
    private long getSizeOfStringStore()
    {
        db.shutdown();
        long size = new File( PATH, "neostore.propertystore.db.strings" ).length();
        db = new GraphDatabaseFactory().newEmbeddedDatabase( PATH );
        return size;
    }

    private long createNode( Map<String, Object> properties )
    {
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        for ( Map.Entry<String, Object> property : properties.entrySet() )
        {
            node.setProperty( property.getKey(), property.getValue() );
        }
        tx.success();
        tx.finish();
        return node.getId();
    }
}
