/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.standard;

public class LuceneIndexAccessorTest
{

    @Test
    public void indexReaderShouldHonorRepeatableReads() throws Exception
    {
        // GIVEN
        accessor.updateAndCommit( asList( add( nodeId, value ) ) );
        IndexReader reader = accessor.newReader();

        // WHEN
        accessor.updateAndCommit( asList( remove( nodeId, value ) ) );

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( reader.lookup( value ) ) );
        reader.close();
    }
    
    @Test
    public void multipleIndexReadersFromDifferentPointsInTimeCanSeeDifferentResults() throws Exception
    {
        // WHEN
        accessor.updateAndCommit( asList( add( nodeId, value ) ) );
        IndexReader firstReader = accessor.newReader();
        accessor.updateAndCommit( asList( add( nodeId2, value ) ) );
        IndexReader secondReader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( firstReader.lookup( value ) ) );
        assertEquals( asSet( nodeId, nodeId2 ), asUniqueSet( secondReader.lookup( value ) ) );
        firstReader.close();
        secondReader.close();
    }
    
    @Test
    public void canAddNewData() throws Exception
    {
        // WHEN
        accessor.updateAndCommit( asList(
                add( nodeId, value ),
                add( nodeId2, value2 ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( reader.lookup( value ) ) );
        reader.close();
    }
    
    @Test
    public void canChangeExistingData() throws Exception
    {
        // GIVEN
        accessor.updateAndCommit( asList( add( nodeId, value ) ) );

        // WHEN
        accessor.updateAndCommit( asList( change( nodeId, value, value2 ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( reader.lookup( value2 ) ) );
        assertEquals( emptySetOf( Long.class ), asUniqueSet( reader.lookup( value ) ) );
        reader.close();
    }
    
    @Test
    public void canRemoveExistingData() throws Exception
    {
        // GIVEN
        accessor.updateAndCommit( asList(
                add( nodeId, value ),
                add( nodeId2, value ) ) );

        // WHEN
        accessor.updateAndCommit( asList( remove( nodeId, value ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId2 ), asUniqueSet( reader.lookup( value ) ) );
        reader.close();
    }
    
    private final long nodeId = 1, nodeId2 = 2;
    private final Object value = "value", value2 = 40;
    private final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
    private final IndexWriterStatus writerLogic = new IndexWriterStatus();
    private final File dir = new File( "dir" );
    private LuceneIndexAccessor accessor;
    private DirectoryFactory.InMemoryDirectoryFactory dirFactory;
    
    @Before
    public void before() throws Exception
    {
        dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        accessor = new NonUniqueLuceneIndexAccessor( documentLogic, standard(), writerLogic, dirFactory, dir );
    }

    @After
    public void after()
    {
        dirFactory.close();
    }

    private NodePropertyUpdate add( long nodeId, Object value )
    {
        return NodePropertyUpdate.add( nodeId, 0, value, new long[0] );
    }
    
    private NodePropertyUpdate remove( long nodeId, Object value )
    {
        return NodePropertyUpdate.remove( nodeId, 0, value, new long[0] );
    }

    private NodePropertyUpdate change( long nodeId, Object valueBefore, Object valueAfter )
    {
        return NodePropertyUpdate.change( nodeId, 0, valueBefore, new long[0], valueAfter, new long[0] );
    }
}
