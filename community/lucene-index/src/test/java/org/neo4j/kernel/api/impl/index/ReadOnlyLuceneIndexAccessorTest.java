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

import org.apache.lucene.index.IndexFileNames;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;

import org.neo4j.function.Function;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ReadOnlyLuceneIndexAccessorTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Rule
    public TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
    private DirectoryFactory directoryFactory = DirectoryFactory.PERSISTENT;
    private File indexDirectory;
    private IndexWriterFactory writerFactory = mock( IndexWriterFactory.class );

    @Before
    public void setUp() throws IOException
    {
        indexDirectory = directory.graphDbDir();
        createEmptySchemaIndex( directoryFactory, indexDirectory );
    }

    @Test
    public void shouldFailCreatingUpdateInReadOnlyMode() throws Exception
    {
        IndexWriterFactory writerFactory = mock( IndexWriterFactory.class );
        try ( LuceneIndexAccessor luceneIndexAccessor = getNonUniqueLuceneIndexAccessor( directoryFactory,
                indexDirectory, documentStructure, true, writerFactory ) )
        {

            expectedException.expect( UnsupportedOperationException.class );
            luceneIndexAccessor.newUpdater( IndexUpdateMode.ONLINE );
        }
    }

    @Test
    public void shouldThrowWhenDropIndexInReadOnlyMode() throws Exception
    {
        IndexWriterFactory writerFactory = mock( IndexWriterFactory.class );
        try ( LuceneIndexAccessor luceneIndexAccessor = getNonUniqueLuceneIndexAccessor( directoryFactory,
                indexDirectory, documentStructure, true, writerFactory ) )
        {
            expectedException.expect( UnsupportedOperationException.class );
            luceneIndexAccessor.drop();
        }
    }

    @Test
    public void canForceInReadOnlyModeAndDontUseWriter() throws Exception
    {
        try ( LuceneIndexAccessor luceneIndexAccessor = getNonUniqueLuceneIndexAccessor( directoryFactory,
                indexDirectory, documentStructure, true, writerFactory ) )
        {
            verifyNoMoreInteractions( writerFactory );
            luceneIndexAccessor.force();
            verifyNoMoreInteractions( writerFactory );
        }
    }

    @Test
    public void snapshotFromReadOnlyIndex() throws Exception
    {
        try ( NonUniqueLuceneIndexAccessor nonUniqueLuceneIndexAccessor =
                      getNonUniqueLuceneIndexAccessor( directoryFactory, indexDirectory ) )
        {
            NodePropertyUpdate add = NodePropertyUpdate.add( 1L, 1, 42, new long[]{1} );
            nonUniqueLuceneIndexAccessor.newUpdater( IndexUpdateMode.ONLINE ).process( add );
        }

        String[] indexFiles = indexDirectory.list( new SegmentGenerationFileFilter() );

        try ( LuceneIndexAccessor luceneIndexAccessor = getNonUniqueLuceneIndexAccessor( directoryFactory,
                indexDirectory, documentStructure, true, writerFactory );
              ResourceIterator<File> indexSnapshot = luceneIndexAccessor.snapshotFiles(); )
        {
            List<String> filesNames = Iterables.toList( Iterables.map( new FileNameExtractor(), indexSnapshot ) );

            assertThat( "Expected snapshot to contain actual index files.",  indexFiles, Matchers
                    .arrayContainingInAnyOrder( filesNames.toArray() ) );
        }
    }

    private void createEmptySchemaIndex( DirectoryFactory directoryFactory, File indexDirectory ) throws IOException
    {
        NonUniqueLuceneIndexAccessor indexAccessor =
                getNonUniqueLuceneIndexAccessor( directoryFactory, indexDirectory );
        indexAccessor.flush();
        indexAccessor.close();
    }

    private NonUniqueLuceneIndexAccessor getNonUniqueLuceneIndexAccessor( DirectoryFactory directoryFactory,
            File indexDirectory ) throws IOException
    {
        return getNonUniqueLuceneIndexAccessor( directoryFactory, indexDirectory, documentStructure, false,
                IndexWriterFactories.reserving() );
    }

    private NonUniqueLuceneIndexAccessor getNonUniqueLuceneIndexAccessor( DirectoryFactory directoryFactory,
            File indexDirectory, LuceneDocumentStructure documentStructure, boolean readOnly,
            IndexWriterFactory<ReservingLuceneIndexWriter> reserving ) throws IOException
    {
        return new NonUniqueLuceneIndexAccessor( documentStructure, readOnly, reserving,
                directoryFactory, indexDirectory, 100 );
    }

    private static class SegmentGenerationFileFilter implements FilenameFilter
    {
        @Override
        public boolean accept( File dir, String name )
        {
            return !IndexFileNames.SEGMENTS_GEN.equals( name );
        }
    }

    private static class FileNameExtractor implements Function<File,String>
    {
        @Override
        public String apply( File file )
        {
            return file.getName();
        }
    }
}
