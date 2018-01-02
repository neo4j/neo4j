/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.test.TargetDirectory;

import static java.util.Arrays.asList;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.reserving;

public class LuceneIndexIT
{
    @Test
    public void shouldProvideStoreSnapshot() throws Exception
    {
        // Given
        updateAndCommit( asList(
                add( nodeId, value ),
                add( nodeId2, value ) ) );
        accessor.force();

        // When & Then
        try ( ResourceIterator<File> snapshot = accessor.snapshotFiles() )
        {
            assertThat( asUniqueSetOfNames( snapshot ), equalTo( asSet( "_0.cfs", "segments_1" ) ) );
        }
    }

    @Test
    public void shouldProvideStoreSnapshotWhenThereAreNoCommits() throws Exception
    {
        // Given
        // A completely un-used index

        // When & Then
        try(ResourceIterator<File> snapshot = accessor.snapshotFiles())
        {
            assertThat( asUniqueSetOfNames( snapshot ), equalTo( emptySetOf( String.class ) ) );
        }
    }

    private Set<String> asUniqueSetOfNames( ResourceIterator<File> files )
    {
        ArrayList<String> out = new ArrayList<>();
        while(files.hasNext())
            out.add( files.next().getName() );
        return asUniqueSet( out );
    }

    private final long nodeId = 1, nodeId2 = 2;
    private final Object value = "value";
    private final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
    private LuceneIndexAccessor accessor;
    private DirectoryFactory dirFactory;

    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Before
    public void before() throws Exception
    {
        dirFactory = DirectoryFactory.PERSISTENT;
        accessor = new NonUniqueLuceneIndexAccessor( documentLogic, false, reserving(),
                dirFactory, testDir.directory(), 100_000 );
    }

    @After
    public void after() throws IOException
    {
        accessor.close();
        dirFactory.close();
    }

    private NodePropertyUpdate add( long nodeId, Object value )
    {
        return NodePropertyUpdate.add( nodeId, 0, value, new long[0] );
    }

    private void updateAndCommit( List<NodePropertyUpdate> nodePropertyUpdates )
            throws IOException, IndexEntryConflictException, IndexCapacityExceededException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( NodePropertyUpdate update : nodePropertyUpdates )
            {
                updater.process( update );
            }
        }
    }
}
