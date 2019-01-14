/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileMoveActionTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    private FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();

    @Test
    public void pageCacheFilesMovedDoNotLeaveOriginal() throws IOException
    {
        // given
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemAbstraction );

        // and
        File pageCacheFile = testDirectory.file( "page-cache-file" );
        PagedFile pagedFile = pageCache.map( pageCacheFile, 100, StandardOpenOption.CREATE );

        // when
        File targetRename = testDirectory.file( "destination" );
        FileMoveAction action = FileMoveAction.moveViaPageCache( pageCacheFile, pageCache );
        pagedFile.close();
        action.move( targetRename );

        // then
        assertFalse( pageCacheFile.exists() );
        assertTrue( targetRename.exists() );
    }

    @Test
    public void nonPageCacheFilesMovedDoNotLeaveOriginal() throws IOException
    {
        // given
        File baseDirectory = testDirectory.directory();
        File sourceDirectory = new File( baseDirectory, "source" );
        File targetDirectory = new File( baseDirectory, "destination" );
        File sourceFile = new File( sourceDirectory, "theFileName" );
        File targetFile = new File( targetDirectory, "theFileName" );
        sourceFile.getParentFile().mkdirs();
        targetDirectory.mkdirs();

        // and sanity check
        assertTrue( sourceFile.createNewFile() );
        assertTrue( sourceFile.exists() );
        assertFalse( targetFile.exists() );

        // when
        FileMoveAction.moveViaFileSystem( sourceFile, sourceDirectory ).move( targetDirectory );

        // then
        assertTrue( targetFile.exists() );
        assertFalse( sourceFile.exists() );
    }

    @Test
    public void nonPageCacheFilesCopiedLeaveOriginal() throws IOException
    {
        // given
        File baseDirectory = testDirectory.directory();
        File sourceDirectory = new File( baseDirectory, "source" );
        File targetDirectory = new File( baseDirectory, "destination" );
        File sourceFile = new File( sourceDirectory, "theFileName" );
        File targetFile = new File( targetDirectory, "theFileName" );
        sourceFile.getParentFile().mkdirs();
        targetDirectory.mkdirs();

        // and sanity check
        assertTrue( sourceFile.createNewFile() );
        assertTrue( sourceFile.exists() );
        assertFalse( targetFile.exists() );

        // when
        FileMoveAction.copyViaFileSystem( sourceFile, sourceDirectory ).move( targetDirectory );

        // then
        assertTrue( targetFile.exists() );
        assertTrue( sourceFile.exists() );
    }

    @Test
    public void symbolicLinkAsTargetShouldNotBreakTheMove() throws Exception
    {
        /*
         * Setup the following structure
         * - realSourceFile: a dummy file serving as the file to copy, the original source
         * - realTargetDirectory: the real directory to move the file into
         * - linkTargetDirectory: a symbolic link pointing to realTargetDirectory.
         */
        String realFileFilename = "realFile"; // we need this for the assert at the end
        Path realSourceFile = Files.createFile( new File( testDirectory.absolutePath(), realFileFilename ).toPath() );
        Path realTargetDirectory = Files.createDirectory( new File( testDirectory.absolutePath(), "realTargetDirectory" ).toPath() );
        Path linkTargetDirectory = Files.createSymbolicLink( new File( testDirectory.absolutePath(), "linkToTarget" ).toPath(), realTargetDirectory );

        /*
         * We now try to copy the realSourceFile to the linkTargetDirectory. This must succeed.
         * As a reminder, the FileMoveAction.copyViaFileSystem() will prepare a file move operation for the real source file
         *  (contained in the top level test directory). The move() call will accept as an argument the symbolic link and
         *  try to move the source in there.
         */
        FileMoveAction.copyViaFileSystem( realSourceFile.toFile(), testDirectory.absolutePath() ).move( linkTargetDirectory.toFile() );

        File target = new File( linkTargetDirectory.toFile(), realFileFilename );
        assertTrue( Files.exists( target.toPath() ) );
    }
}
