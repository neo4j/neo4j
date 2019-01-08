/*
 * Copyright (c) 2002-2019 "Neo Technology,"
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
package org.neo4j.com.storecopy;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertTrue;

public class FileMoveActionTest
{
    @Rule
    public TestDirectory dir = TestDirectory.testDirectory();

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
        Path realSourceFile = Files.createFile( new File( dir.absolutePath(), realFileFilename ).toPath() );
        Path realTargetDirectory = Files.createDirectory( new File( dir.absolutePath(), "realTargetDirectory" ).toPath() );
        Path linkTargetDirectory = Files.createSymbolicLink( new File( dir.absolutePath(), "linkToTarget" ).toPath(), realTargetDirectory );

        /*
         * We now try to copy the realSourceFile to the linkTargetDirectory. This must succeed.
         * As a reminder, the FileMoveAction.copyViaFileSystem() will prepare a file move operation for the real source file
         *  (contained in the top level test directory). The move() call will accept as an argument the symbolic link and
         *  try to move the source in there.
         */
        FileMoveAction.copyViaFileSystem( realSourceFile.toFile(), dir.absolutePath() ).move( linkTargetDirectory.toFile() );

        File target = new File( linkTargetDirectory.toFile(), realFileFilename );
        assertTrue( Files.exists( target.toPath() ) );
    }
}
