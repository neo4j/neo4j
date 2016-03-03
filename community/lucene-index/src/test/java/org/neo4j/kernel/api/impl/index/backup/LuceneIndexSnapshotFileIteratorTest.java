/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index.backup;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.test.TargetDirectory;

import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;

public class LuceneIndexSnapshotFileIteratorTest
{
    @Rule
    public final TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    private File indexDir;
    private Directory dir;
    private IndexWriter writer;

    @Before
    public void initializeLuceneResources() throws IOException
    {
        indexDir = testDir.directory();
        dir = new RAMDirectory();
        writer = new IndexWriter( dir, IndexWriterConfigs.standard() );
    }

    @After
    public void closeLuceneResources() throws IOException
    {
        IOUtils.closeAll( writer, dir );
    }

    @Test
    public void shouldReturnRealSnapshotIfIndexAllowsIt() throws IOException
    {
        insertRandomDocuments( writer );

        Set<String> files = listDir( dir );
        assertFalse( files.isEmpty() );

        try ( ResourceIterator<File> snapshot = LuceneIndexSnapshotFileIterator.forIndex( indexDir, writer ) )
        {
            Set<String> snapshotFiles = Iterators.asList( snapshot ).stream().map( File::getName ).collect( toSet() );
            assertEquals( files, snapshotFiles );
        }
    }

    @Test
    public void shouldReturnEmptyIteratorWhenNoCommitsHaveBeenMade() throws IOException
    {
        try ( ResourceIterator<File> snapshot = LuceneIndexSnapshotFileIterator.forIndex( indexDir, writer ) )
        {
            assertFalse( snapshot.hasNext() );
        }
    }

    private static void insertRandomDocuments( IndexWriter writer ) throws IOException
    {
        Document doc = new Document();
        doc.add( new StringField( "a", "b", Field.Store.YES ) );
        doc.add( new StringField( "c", "d", Field.Store.NO ) );
        writer.addDocument( doc );
        writer.commit();
    }

    private static Set<String> listDir( Directory dir ) throws IOException
    {
        String[] files = dir.listAll();
        return Stream.of( files ).collect( toSet() );
    }
}
