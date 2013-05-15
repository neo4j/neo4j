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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.standard;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.junit.Test;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

public class UniquenessEnforcingLuceneIndexPopulatorTest
{
    @Test
    public void shouldAddUniqueEntries() throws Exception
    {
        // given
        DirectoryFactory.InMemoryDirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        File indexDirectory = new File( "whatever" );
        final LuceneSchemaIndexProvider.DocumentLogic documentLogic = new LuceneSchemaIndexProvider.DocumentLogic();
        UniquenessEnforcingLuceneIndexPopulator populator = new UniquenessEnforcingLuceneIndexPopulator( 100,
                documentLogic, standard(),
                directoryFactory, indexDirectory );
        populator.create();

        // when
        populator.add( 1, "value1" );
        populator.add( 2, "value2" );

        populator.close( true );

        // then
        final IndexSearcher searcher = new SearcherManager(
                standard().create( directoryFactory.open( indexDirectory ) ), true, new SearcherFactory() ).acquire();

        ArrayList<Long> nodeIds = new ArrayList<Long>();
        searcher.search( documentLogic.newQuery( "value1" ), new AllNodesCollector( documentLogic, nodeIds ) );
        assertEquals( asList( 1l ), nodeIds );
    }

    @Test
    public void shouldUpdateUniqueEntries() throws Exception
    {
        // given
        DirectoryFactory.InMemoryDirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        File indexDirectory = new File( "whatever" );
        final LuceneSchemaIndexProvider.DocumentLogic documentLogic = new LuceneSchemaIndexProvider.DocumentLogic();
        UniquenessEnforcingLuceneIndexPopulator populator = new UniquenessEnforcingLuceneIndexPopulator( 100,
                documentLogic, standard(),
                directoryFactory, indexDirectory );
        populator.create();

        // when
        populator.add( 1, "value1" );
        populator.add( 2, "value2" );
        populator.update( asList( NodePropertyUpdate.add( 3, 100, "value3", new long[]{1000} ) ) );

        populator.close( true );

        // then
        final IndexSearcher searcher = new SearcherManager(
                standard().create( directoryFactory.open( indexDirectory ) ), true, new SearcherFactory() ).acquire();

        ArrayList<Long> nodeIds = new ArrayList<Long>();
        searcher.search( documentLogic.newQuery( "value3" ), new AllNodesCollector( documentLogic, nodeIds ) );
        assertEquals( asList( 3l ), nodeIds );
    }

    @Test
    public void shouldRejectEntryWithAlreadyIndexedValue() throws Exception
    {
        // given
        UniquenessEnforcingLuceneIndexPopulator populator = new UniquenessEnforcingLuceneIndexPopulator( 100,
                new LuceneSchemaIndexProvider.DocumentLogic(), standard(),
                new DirectoryFactory.InMemoryDirectoryFactory(), new File( "whatever" ) );
        populator.create();
        populator.add( 1, "value1" );

        // when
        try
        {
            populator.add( 2, "value1" );

            fail( "should have thrown exception" );
        }
        // then
        catch ( IndexEntryConflictException conflict )
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
        final LuceneSchemaIndexProvider.DocumentLogic documentLogic = new LuceneSchemaIndexProvider.DocumentLogic();
        UniquenessEnforcingLuceneIndexPopulator populator = new UniquenessEnforcingLuceneIndexPopulator( 2,
                documentLogic, standard(),
                directoryFactory, indexDirectory );
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
        catch ( IndexEntryConflictException conflict )
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
        UniquenessEnforcingLuceneIndexPopulator populator = new UniquenessEnforcingLuceneIndexPopulator( 100,
                new LuceneSchemaIndexProvider.DocumentLogic(), standard(),
                new DirectoryFactory.InMemoryDirectoryFactory(), new File( "whatever" ) );
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
        catch ( IndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( "value1", conflict.getPropertyValue() );
            assertEquals( 2, conflict.getAddedNodeId() );
        }
    }

    private static class AllNodesCollector extends Collector
    {
        private final List<Long> nodeIds;
        private final LuceneSchemaIndexProvider.DocumentLogic documentLogic;
        public IndexReader reader;

        public AllNodesCollector( LuceneSchemaIndexProvider.DocumentLogic documentLogic, ArrayList<Long> nodeIds )
        {
            this.documentLogic = documentLogic;
            this.nodeIds = nodeIds;
        }

        @Override
        public void setScorer( Scorer scorer ) throws IOException
        {
        }

        @Override
        public void collect( int doc ) throws IOException
        {
            nodeIds.add( documentLogic.getNodeId( reader.document( doc ) ) );
        }

        @Override
        public void setNextReader( IndexReader reader, int docBase ) throws IOException
        {
            this.reader = reader;
        }

        @Override
        public boolean acceptsDocsOutOfOrder()
        {
            return true;
        }
    }
}
