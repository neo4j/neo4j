/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * To support block device storage for causal clustering, it is important that the interaction with files go through
 * either the normal file system, and/or through the page cache, depending on the file.
 */
public class StoreFilesTest
{
    protected TestDirectory testDirectory;
    protected Supplier<FileSystemAbstraction> fileSystemRule;
    protected EphemeralFileSystemRule hiddenFileSystemRule;
    protected PageCacheRule pageCacheRule;

    @Rule
    public RuleChain rules;

    private FileSystemAbstraction fs;
    private EphemeralFileSystemAbstraction pc;
    private PageCache pageCache;
    private StoreFiles storeFiles;

    public StoreFilesTest()
    {
        createRules();
    }

    protected void createRules()
    {
        testDirectory = TestDirectory.testDirectory( StoreFilesTest.class );
        EphemeralFileSystemRule ephemeralFileSystemRule = new EphemeralFileSystemRule();
        fileSystemRule = ephemeralFileSystemRule;
        hiddenFileSystemRule = new EphemeralFileSystemRule();
        pageCacheRule = new PageCacheRule();
        rules = RuleChain.outerRule( ephemeralFileSystemRule )
                         .around( testDirectory )
                         .around( hiddenFileSystemRule )
                         .around( pageCacheRule );
    }

    @Before
    public void setUp() throws Exception
    {
        fs = fileSystemRule.get();
        pc = hiddenFileSystemRule.get();
        pageCache = pageCacheRule.getPageCache( pc );
        storeFiles = new StoreFiles( fs, pageCache );
    }

    private void createOnFileSystem( File file ) throws IOException
    {
        createFile( fs, file );
    }

    private void createOnPageCache( File file ) throws IOException
    {
        createFile( hiddenFileSystemRule.get(), file );
    }

    private void createFile( FileSystemAbstraction fs, File file ) throws IOException
    {
        fs.mkdirs( file.getParentFile() );
        fs.open( file, "rw" ).close();
    }

    protected File getBaseDir()
    {
        return new File( testDirectory.directory(), "dir" );
    }

    @Test
    public void deleteMustRecursivelyRemoveFilesInGivenDirectory() throws Exception
    {
        File dir = getBaseDir();
        File a = new File( dir, "a" );
        File b = new File( dir, "b" );

        createOnFileSystem( a );
        createOnPageCache( b );
        assertTrue( fs.fileExists( a ) );
        assertTrue( pc.fileExists( b ) );

        storeFiles.delete( dir );

        assertFalse( fs.fileExists( a ) );
        assertFalse( pc.fileExists( b ) );
    }

    @Test
    public void deleteMustNotDeleteIgnoredFiles() throws Exception
    {
        File dir = getBaseDir();
        File a = new File( dir, "a" );
        File b = new File( dir, "b" );
        File c = new File( dir, "c" );
        File d = new File( dir, "d" );

        createOnFileSystem( a );
        createOnFileSystem( c );
        createOnPageCache( b );
        createOnPageCache( d );

        FilenameFilter filter = ( directory, name ) -> !name.equals( "c" ) && !name.equals( "d" );
        storeFiles = new StoreFiles( fs, pageCache, filter );
        storeFiles.delete( dir );

        assertFalse( fs.fileExists( a ) );
        assertFalse( pc.fileExists( b ) );
        assertTrue( fs.fileExists( c ) );
        assertTrue( pc.fileExists( d ) );
    }

    @Test
    public void deleteMustNotDeleteFilesInIgnoredDirectories() throws Exception
    {
        File dir = getBaseDir();
        File ignore = new File( dir, "ignore" );
        File a = new File( dir, "a" );
        File b = new File( dir, "b" );
        File c = new File( ignore, "c" );
        File d = new File( ignore, "d" );

        createOnFileSystem( a );
        createOnFileSystem( c );
        createOnPageCache( b );
        createOnPageCache( d );

        FilenameFilter filter = ( directory, name ) -> !name.startsWith( "ignore" );
        storeFiles = new StoreFiles( fs, pageCache, filter );
        storeFiles.delete( dir );

        assertFalse( fs.fileExists( a ) );
        assertFalse( pc.fileExists( b ) );
        assertTrue( fs.fileExists( c ) );
        assertTrue( pc.fileExists( d ) );
    }

    @Test
    public void deleteMustSilentlyIgnoreMissingDirectories() throws Exception
    {
        File dir = getBaseDir();
        File sub = new File( dir, "sub" );

        storeFiles.delete( sub );
    }

    @Test
    public void mustMoveFilesToTargetDirectory() throws Exception
    {
        File base = getBaseDir();
        File src = new File( base, "src" );
        File tgt = new File( base, "tgt" );
        File a = new File( src, "a" );
        File b = new File( src, "b" );

        createOnFileSystem( a );
        createOnPageCache( b );

        // Ensure the 'tgt' directory exists
        createOnFileSystem( new File( tgt, ".fs-ignore" ) );
        createOnPageCache( new File( tgt, ".pc-ignore" ) );

        storeFiles.moveTo( src, tgt );

        assertFalse( fs.fileExists( a ) );
        assertFalse( pc.fileExists( b ) );
        assertTrue( fs.fileExists( new File( tgt, "a" ) ) );
        assertTrue( pc.fileExists( new File( tgt, "b" ) ) );
    }

    @Test
    public void movedFilesMustRetainTheirRelativePaths() throws Exception
    {
        File base = getBaseDir();
        File src = new File( base, "src" );
        File tgt = new File( base, "tgt" );
        File dir = new File( src, "dir" );
        File a = new File( dir, "a" );
        File b = new File( dir, "b" );

        createOnFileSystem( a );
        createOnPageCache( b );

        // Ensure the 'tgt' directory exists
        createOnFileSystem( new File( tgt, ".fs-ignore" ) );
        createOnPageCache( new File( tgt, ".pc-ignore" ) );

        storeFiles.moveTo( src, tgt );

        assertFalse( fs.fileExists( a ) );
        assertFalse( pc.fileExists( b ) );
        assertTrue( fs.fileExists( new File( new File( tgt, "dir" ), "a" ) ) );
        assertTrue( pc.fileExists( new File( new File( tgt, "dir" ), "b" ) ) );
    }

    @Test
    public void moveMustIgnoreFilesFilteredOut() throws Exception
    {
        File base = getBaseDir();
        File src = new File( base, "src" );
        File a = new File( src, "a" );
        File b = new File( src, "b" );
        File ignore = new File( src, "ignore" );
        File c = new File( ignore, "c" );
        File d = new File( ignore, "d" );
        File tgt = new File( base, "tgt" );

        createOnFileSystem( a );
        createOnPageCache( b );
        createOnFileSystem( c );
        createOnPageCache( d );

        // Ensure the 'tgt' directory exists
        createOnFileSystem( new File( tgt, ".fs-ignore" ) );
        createOnPageCache( new File( tgt, ".pc-ignore" ) );

        FilenameFilter filter = ( directory, name ) -> !name.startsWith( "ignore" );
        storeFiles = new StoreFiles( fs, pageCache, filter );
        storeFiles.moveTo( src, tgt );

        assertFalse( fs.fileExists( a ) );
        assertFalse( pc.fileExists( b ) );
        assertTrue( fs.fileExists( c ) );
        assertTrue( pc.fileExists( d ) );
        assertTrue( fs.fileExists( new File( tgt, "a" ) ) );
        assertTrue( pc.fileExists( new File( tgt, "b" ) ) );
    }

    @Test
    public void isEmptyMustFindFilesBothOnFileSystemAndPageCache() throws Exception
    {
        File dir = getBaseDir();
        File ignore = new File( dir, "ignore" );
        File a = new File( dir, "a" );
        File b = new File( dir, "b" );
        File c = new File( dir, "c" );
        File d = new File( dir, "d" );

        createOnFileSystem( a );
        createOnFileSystem( c );
        createOnFileSystem( ignore );
        createOnPageCache( b );
        createOnPageCache( d );
        createOnPageCache( ignore );

        FilenameFilter filter = ( directory, name ) -> !name.startsWith( "ignore" );
        storeFiles = new StoreFiles( fs, pageCache, filter );

        List<File> filesOnFilesystem = Arrays.asList( a, c );
        List<File> fileOnFilesystem = Arrays.asList( a );
        List<File> filesOnPageCache = Arrays.asList( b, d );
        List<File> fileOnPageCache = Arrays.asList( b );
        List<File> ingore = Arrays.asList( ignore );

        assertFalse( storeFiles.isEmpty( dir, filesOnFilesystem ) );
        assertFalse( storeFiles.isEmpty( dir, fileOnFilesystem ) );
        assertFalse( storeFiles.isEmpty( dir, filesOnPageCache ) );
        assertFalse( storeFiles.isEmpty( dir, fileOnPageCache ) );
        assertTrue( storeFiles.isEmpty( dir, Collections.emptyList() ) );
        assertTrue( storeFiles.isEmpty( dir, ingore ) );
    }

    @Test
    public void mustReadStoreId() throws Exception
    {
        File dir = getBaseDir();
        File neostore = new File( dir, MetaDataStore.DEFAULT_NAME );
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        long time = rng.nextLong();
        long randomNumber = rng.nextLong();
        long upgradeTime = rng.nextLong();
        long upgradeTransactionId = rng.nextLong();

        createOnPageCache( neostore );

        MetaDataStore.setRecord( pageCache, neostore, Position.TIME, time );
        MetaDataStore.setRecord( pageCache, neostore, Position.RANDOM_NUMBER, randomNumber );
        MetaDataStore.setRecord( pageCache, neostore, Position.STORE_VERSION, rng.nextLong() );
        MetaDataStore.setRecord( pageCache, neostore, Position.UPGRADE_TIME, upgradeTime );
        MetaDataStore.setRecord( pageCache, neostore, Position.UPGRADE_TRANSACTION_ID, upgradeTransactionId );

        StoreId storeId = storeFiles.readStoreId( dir );

        assertThat( storeId.getCreationTime(), is( time ) );
        assertThat( storeId.getRandomId(), is( randomNumber ) );
        assertThat( storeId.getUpgradeTime(), is( upgradeTime ) );
        assertThat( storeId.getUpgradeId(), is( upgradeTransactionId ) );
    }
}
