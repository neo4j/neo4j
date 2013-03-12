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

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;
import static org.neo4j.kernel.api.impl.index.DirectoryFactory.IN_MEMORY;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.standard;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider.DocumentLogic;

public class RefreshableIndexSearcherTest
{
    @Test
    public void changedIndexShouldFindNewUpdates() throws Exception
    {
        // WHEN
        writer.addDocument( logic.newDocument( nodeId, value ) );
        index.refresh();
        IndexSearcher searcher = index.getUpToDateSearcher();
        
        // THEN
        assertHits( searcher, value, nodeId );
    }
    
    @Test
    public void oldSearcherReferenceShouldSeeSameValuesAsSeenBefore() throws Exception
    {
        // GIVEN
        writer.addDocument( logic.newDocument( nodeId, value ) );
        index.refresh();
        IndexSearcher searcher = index.getUpToDateSearcher();
        searcher.search( logic.newQuery( value ), 10 );

        // WHEN
        writer.deleteDocuments( logic.newQueryForChangeOrRemove( nodeId, value ) );
        // don't call indexChanged()

        // THEN
        assertHits( searcher, value, nodeId );
    }

    private final String value = "value";
    private final long nodeId = 1;
    private final DocumentLogic logic = new DocumentLogic();
    private IndexWriter writer;
    private RefreshableIndexSearcher index;
    
    @Before
    public void before() throws Exception
    {
        writer = standard( IN_MEMORY ).create( new File( "whatever" ) );
        index = new RefreshableIndexSearcher( writer );
    }

    private void assertHits( IndexSearcher searcher, String value, Long... nodeIds ) throws IOException
    {
        TopDocs hits = searcher.search( logic.newQuery( value ), 10 );
        assertEquals( nodeIds.length, hits.scoreDocs.length );
        assertEquals( asSet( nodeIds ), asUniqueSet( logic.getNodeId( searcher.doc( hits.scoreDocs[0].doc ) ) ) );
    }
}
