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
import java.util.List;

import org.junit.Test;

import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.kernel.api.impl.index.AllNodesCollector.getAllNodes;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.standard;

public class UniqueLuceneIndexPopulatorTest
{
    @Test
    public void shouldAddUniqueEntries() throws Exception
    {
        // given
        DirectoryFactory.InMemoryDirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        File indexDirectory = new File( "whatever" );
        final LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
        UniqueLuceneIndexPopulator populator = new UniqueLuceneIndexPopulator( 100,
                documentStructure, standard(),
                new IndexWriterStatus(), directoryFactory, indexDirectory );
        populator.create();

        // when
        populator.add( 1, "value1" );
        populator.add( 2, "value2" );

        populator.close( true );

        // then
        List<Long> nodeIds = getAllNodes( directoryFactory, indexDirectory, "value1" );
        assertEquals( asList( 1l ), nodeIds );
    }

    @Test
    public void shouldUpdateUniqueEntries() throws Exception
    {
        // given
        DirectoryFactory.InMemoryDirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        File indexDirectory = new File( "whatever" );
        final LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
        UniqueLuceneIndexPopulator populator = new UniqueLuceneIndexPopulator( 100,
                documentStructure, standard(),
                new IndexWriterStatus(), directoryFactory, indexDirectory );
        populator.create();

        // when
        populator.add( 1, "value1" );
        populator.add( 2, "value2" );
        populator.update( asList( NodePropertyUpdate.add( 3, 100, "value3", new long[]{1000} ) ) );

        populator.close( true );

        // then
        List<Long> nodeIds = getAllNodes( directoryFactory, indexDirectory, "value3" );
        assertEquals( asList( 3l ), nodeIds );
    }

    @Test
    public void shouldRejectEntryWithAlreadyIndexedValue() throws Exception
    {
        // given
        UniqueLuceneIndexPopulator populator = new UniqueLuceneIndexPopulator( 100,
                new LuceneDocumentStructure(), standard(),
                new IndexWriterStatus(), new DirectoryFactory.InMemoryDirectoryFactory(), new File( "whatever" )
        );
        populator.create();
        populator.add( 1, "value1" );

        // when
        try
        {
            populator.add( 2, "value1" );

            fail( "should have thrown exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( "value1", conflict.getPropertyValue() );
            assertEquals( 2, conflict.getAddedNodeId() );
        }
    }

    @Test
    public void shouldRejectEntryWithAlreadyIndexedValueFromPreviousBatch() throws Exception
    {
        DirectoryFactory.InMemoryDirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        File indexDirectory = new File( "whatever" );
        final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
        UniqueLuceneIndexPopulator populator = new UniqueLuceneIndexPopulator( 2,
                documentLogic, standard(),
                new IndexWriterStatus(), directoryFactory, indexDirectory );
        populator.create();

        populator.add( 1, "value1" );
        populator.add( 2, "value2" );

        // when
        try
        {
            populator.add( 3, "value1" );

            fail( "should have thrown exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( "value1", conflict.getPropertyValue() );
            assertEquals( 3, conflict.getAddedNodeId() );
        }
    }

    @Test
    public void shouldRejectUpdateWithAlreadyIndexedValue() throws Exception
    {
        // given
        UniqueLuceneIndexPopulator populator = new UniqueLuceneIndexPopulator( 100,
                new LuceneDocumentStructure(), standard(),
                new IndexWriterStatus(), new DirectoryFactory.InMemoryDirectoryFactory(), new File( "whatever" )
        );
        populator.create();
        populator.add( 1, "value1" );
        populator.add( 2, "value2" );

        // when
        try
        {
            populator.update( asList(  NodePropertyUpdate.add( 2, 100, "value1", new long[]{1000} )  ) );

            fail( "should have thrown exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( "value1", conflict.getPropertyValue() );
            assertEquals( 2, conflict.getAddedNodeId() );
        }
    }
}
