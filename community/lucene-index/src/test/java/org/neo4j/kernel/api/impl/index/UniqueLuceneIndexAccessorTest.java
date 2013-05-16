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

import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.api.impl.index.AllNodesCollector.getAllNodes;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.standard;

public class UniqueLuceneIndexAccessorTest
{

    @Test
    public void shouldAddUniqueEntries() throws Exception
    {
        // given
        DirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        File indexDirectory = new File( "index1" );
        LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
        UniqueLuceneIndexAccessor accessor = new UniqueLuceneIndexAccessor( documentStructure, standard(),
                                                                            new IndexWriterStatus(),
                                                                            directoryFactory,
                                                                            indexDirectory );

        // when
        accessor.updateAndCommit( asList( add( 1, "value1" ),
                                          add( 2, "value2" ) ) );
        accessor.updateAndCommit( asList( add( 3, "value3" ) ) );
        accessor.close();

        // then
        List<Long> nodeIds = getAllNodes( documentStructure, directoryFactory.open( indexDirectory ), "value1" );
        assertEquals( asList( 1l ), nodeIds );
    }

    @Test
    public void shouldRejectEntryWithAlreadyIndexedValue() throws Exception
    {
        // given
        DirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        File indexDirectory = new File( "index1" );
        LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
        IndexAccessor accessor = new UniqueLuceneIndexAccessor( documentStructure, standard(),
                                                                new IndexWriterStatus(),
                                                                directoryFactory,
                                                                indexDirectory );

        accessor.updateAndCommit( asList( add( 1, "value1" ) ) );

        // when
        try
        {
            accessor.updateAndCommit( asList( add( 2, "value1" ) ) );

            fail( "expected exception" );
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
    public void shouldRejectEntriesInSameTransactionWithDuplicatedIndexedValues() throws Exception
    {
        // given
        DirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        File indexDirectory = new File( "index1" );
        LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
        IndexAccessor accessor = new UniqueLuceneIndexAccessor( documentStructure, standard(),
                                                                new IndexWriterStatus(),
                                                                directoryFactory,
                                                                indexDirectory );

        // when
        try
        {
            accessor.updateAndCommit( asList( add( 1, "value1" ),
                                              add( 2, "value1" ) ) );

            fail( "expected exception" );
        }
        // then
        catch ( IndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( "value1", conflict.getPropertyValue() );
            assertEquals( 2, conflict.getAddedNodeId() );
        }
    }

    private NodePropertyUpdate add( int nodeId, Object propertyValue )
    {
        return NodePropertyUpdate.add( nodeId, 100, propertyValue, new long[]{1000} );
    }
}
