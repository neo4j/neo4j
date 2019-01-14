/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.test.rule.TestDirectory;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ReadOnlyIndexSnapshotFileIteratorTest
{
    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory();

    protected File indexDir;
    protected Directory dir;

    @Before
    public void setUp() throws IOException
    {
        indexDir = testDir.directory();
        dir = DirectoryFactory.PERSISTENT.open( indexDir );
    }

    @After
    public void tearDown() throws IOException
    {
        IOUtils.closeAll( dir );
    }

    @Test
    public void shouldReturnRealSnapshotIfIndexAllowsIt() throws IOException
    {
        prepareIndex();

        Set<String> files = listDir( dir );
        assertFalse( files.isEmpty() );

        try ( ResourceIterator<File> snapshot = makeSnapshot() )
        {
            Set<String> snapshotFiles = snapshot.stream().map( File::getName ).collect( toSet() );
            assertEquals( files, snapshotFiles );
        }
    }

    @Test
    public void shouldReturnEmptyIteratorWhenNoCommitsHaveBeenMade() throws IOException
    {
        try ( ResourceIterator<File> snapshot = makeSnapshot() )
        {
            assertFalse( snapshot.hasNext() );
        }
    }

    private void prepareIndex() throws IOException
    {
        try ( IndexWriter writer = new IndexWriter( dir, IndexWriterConfigs.standard() ) )
        {
            insertRandomDocuments( writer );
        }
    }

    protected ResourceIterator<File> makeSnapshot() throws IOException
    {
        return LuceneIndexSnapshots.forIndex( indexDir, dir );
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
        return Stream.of( files )
                .filter( file -> !IndexWriter.WRITE_LOCK_NAME.equals( file ) )
                .collect( toSet() );
    }

}
