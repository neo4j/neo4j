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
package org.neo4j.kernel.api.impl.index.storage;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.helpers.ArrayUtil;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class PartitionedIndexStorageTest
{
    @Rule
    public final DefaultFileSystemRule fsRule = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory( getClass(), fsRule.get() );

    private FileSystemAbstraction fs;
    private PartitionedIndexStorage storage;

    @Before
    public void createIndexStorage()
    {
        fs = fsRule.get();
        storage = new PartitionedIndexStorage( getOrCreateDirFactory( fs ), fs, testDir.graphDbDir() );
    }

    @Test
    public void prepareFolderCreatesFolder() throws IOException
    {
        File folder = createRandomFolder( testDir.graphDbDir() );

        storage.prepareFolder( folder );

        assertTrue( fs.fileExists( folder ) );
    }

    @Test
    public void prepareFolderRemovesFromFileSystem() throws IOException
    {
        File folder = createRandomFolder( testDir.graphDbDir() );
        createRandomFilesAndFolders( folder );

        storage.prepareFolder( folder );

        assertTrue( fs.fileExists( folder ) );
        assertTrue( ArrayUtil.isEmpty( fs.listFiles( folder ) ) );
    }

    @Test
    public void prepareFolderRemovesFromLucene() throws IOException
    {
        File folder = createRandomFolder( testDir.graphDbDir() );
        Directory dir = createRandomLuceneDir( folder );

        assertFalse( ArrayUtil.isEmpty( dir.listAll() ) );

        storage.prepareFolder( folder );

        assertTrue( fs.fileExists( folder ) );
        assertTrue( ArrayUtil.isEmpty( dir.listAll() ) );
    }

    @Test
    public void openIndexDirectoriesForEmptyIndex() throws IOException
    {
        File indexFolder = storage.getIndexFolder();

        Map<File,Directory> directories = storage.openIndexDirectories();

        assertTrue( directories.isEmpty() );
    }

    @Test
    public void openIndexDirectories() throws IOException
    {
        File indexFolder = storage.getIndexFolder();
        createRandomLuceneDir( indexFolder ).close();
        createRandomLuceneDir( indexFolder ).close();

        Map<File,Directory> directories = storage.openIndexDirectories();
        try
        {
            assertEquals( 2, directories.size() );
            for ( Directory dir : directories.values() )
            {
                assertFalse( ArrayUtil.isEmpty( dir.listAll() ) );
            }
        }
        finally
        {
            IOUtils.closeAll( directories.values() );
        }
    }

    @Test
    public void listFoldersForEmptyFolder() throws IOException
    {
        File indexFolder = storage.getIndexFolder();
        fs.mkdirs( indexFolder );

        List<File> folders = storage.listFolders();

        assertTrue( folders.isEmpty() );
    }

    @Test
    public void listFolders() throws IOException
    {
        File indexFolder = storage.getIndexFolder();
        fs.mkdirs( indexFolder );

        createRandomFile( indexFolder );
        createRandomFile( indexFolder );
        File folder1 = createRandomFolder( indexFolder );
        File folder2 = createRandomFolder( indexFolder );

        List<File> folders = storage.listFolders();

        assertEquals( asSet( folder1, folder2 ), new HashSet<>( folders ) );
    }

    @Test
    public void shouldListIndexPartitionsSorted() throws Exception
    {
        // GIVEN
        try ( FileSystemAbstraction scramblingFs = new DefaultFileSystemAbstraction()
                {
                    @Override
                    public File[] listFiles( File directory )
                    {
                        List<File> files = asList( super.listFiles( directory ) );
                        Collections.shuffle( files );
                        return files.toArray( new File[files.size()] );
                    }
                } )
        {
            PartitionedIndexStorage myStorage = new PartitionedIndexStorage( getOrCreateDirFactory( scramblingFs ),
                    scramblingFs, testDir.graphDbDir() );
            File parent = myStorage.getIndexFolder();
            int directoryCount = 10;
            for ( int i = 0; i < directoryCount; i++ )
            {
                scramblingFs.mkdirs( new File( parent, String.valueOf( i + 1 ) ) );
            }

            // WHEN
            Map<File,Directory> directories = myStorage.openIndexDirectories();

            // THEN
            assertEquals( directoryCount, directories.size() );
            int previous = 0;
            for ( Map.Entry<File,Directory> directory : directories.entrySet() )
            {
                int current = parseInt( directory.getKey().getName() );
                assertTrue( "Wanted directory " + current + " to have higher id than previous " + previous,
                        current > previous );
                previous = current;
            }
        }
    }

    private void createRandomFilesAndFolders( File rootFolder ) throws IOException
    {
        int count = ThreadLocalRandom.current().nextInt( 10 ) + 1;
        for ( int i = 0; i < count; i++ )
        {
            if ( ThreadLocalRandom.current().nextBoolean() )
            {
                createRandomFile( rootFolder );
            }
            else
            {
                createRandomFolder( rootFolder );
            }
        }
    }

    private Directory createRandomLuceneDir( File rootFolder ) throws IOException
    {
        File folder = createRandomFolder( rootFolder );
        DirectoryFactory directoryFactory = getOrCreateDirFactory( fs );
        Directory directory = directoryFactory.open( folder );
        try ( IndexWriter writer = new IndexWriter( directory, IndexWriterConfigs.standard() ) )
        {
            writer.addDocument( randomDocument() );
            writer.commit();
        }
        return directory;
    }

    private void createRandomFile( File rootFolder ) throws IOException
    {
        File file = new File( rootFolder, RandomStringUtils.randomNumeric( 5 ) );
        try ( StoreChannel channel = fs.create( file ) )
        {
            channel.writeAll( ByteBuffer.allocate( 100 ) );
        }
    }

    private File createRandomFolder( File rootFolder ) throws IOException
    {
        File folder = new File( rootFolder, RandomStringUtils.randomNumeric( 5 ) );
        fs.mkdirs( folder );
        return folder;
    }

    private static Document randomDocument()
    {
        Document doc = new Document();
        doc.add( new StringField( "field", RandomStringUtils.randomNumeric( 5 ), Field.Store.YES ) );
        return doc;
    }

    private static DirectoryFactory getOrCreateDirFactory( FileSystemAbstraction fs )
    {
        return fs.getOrCreateThirdPartyFileSystem( DirectoryFactory.class,
                clazz -> new DirectoryFactory.InMemoryDirectoryFactory() );
    }
}
