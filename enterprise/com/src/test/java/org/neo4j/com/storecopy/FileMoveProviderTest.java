/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileMoveProviderTest
{
    private DefaultFileSystemAbstraction defaultFileSystemAbstraction = new DefaultFileSystemAbstraction();

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( defaultFileSystemAbstraction );

    private FileMoveProvider subject;

    @Before
    public void setup()
    {
        subject = new FileMoveProvider( defaultFileSystemAbstraction );
    }

    @Test
    public void moveSingleFiles() throws IOException
    {
        // given
        File sharedParent = testDirectory.cleanDirectory( "shared_parent" );
        File sourceParent = new File( sharedParent, "source" );
        assertTrue( sourceParent.mkdirs() );
        File sourceFile = new File( sourceParent, "file.txt" );
        assertTrue( sourceFile.createNewFile() );
        writeToFile( sourceFile, "Garbage data" );
        File targetParent = new File( sharedParent, "target" );
        assertTrue( targetParent.mkdirs() );
        File targetFile = new File( targetParent, "file.txt" );

        // when
        subject.traverseForMoving( sourceFile ).forEach( moveToDirectory( targetParent ) );

        // then
        assertEquals( "Garbage data", readFromFile( targetFile ) );
    }

    @Test
    public void singleDirectoriesAreNotMoved() throws IOException
    {
        // given
        File sharedParent = testDirectory.cleanDirectory( "shared_parent" );
        File sourceParent = new File( sharedParent, "source" );
        assertTrue( sourceParent.mkdirs() );
        File sourceDirectory = new File( sourceParent, "directory" );
        assertTrue( sourceDirectory.mkdirs() );

        // and
        File targetParent = new File( sharedParent, "target" );
        assertTrue( targetParent.mkdirs() );
        File targetDirectory = new File( targetParent, "directory" );
        assertFalse( targetDirectory.exists() );

        // when
        subject.traverseForMoving( sourceParent ).forEach( moveToDirectory( targetDirectory ) );

        // then
        assertTrue( sourceDirectory.exists() );
        assertFalse( targetDirectory.exists() );
    }

    @Test
    public void moveNestedFiles() throws IOException
    {
        // given
        File sharedParent = testDirectory.cleanDirectory( "shared_parent" );
        File sourceParent = new File( sharedParent, "source" );
        assertTrue( sourceParent.mkdirs() );
        File targetParent = new File( sharedParent, "target" );
        assertTrue( targetParent.mkdirs() );

        // and
        File dirA = new File( sourceParent, "A" );
        assertTrue( dirA.mkdirs() );
        File nestedFileOne = new File( dirA, "file.txt" );
        assertTrue( nestedFileOne.createNewFile() );
        File dirB = new File( sourceParent, "B" );
        assertTrue( dirB.mkdirs() );
        File nestedFileTwo = new File( dirB, "file.txt" );
        assertTrue( nestedFileTwo.createNewFile() );
        writeToFile( nestedFileOne, "This is the file contained in directory A" );
        writeToFile( nestedFileTwo, "This is the file contained in directory B" );

        // and
        File targetFileOne = new File( targetParent, "A/file.txt" );
        File targetFileTwo = new File( targetParent, "B/file.txt" );

        // when
        subject.traverseForMoving( sourceParent ).forEach( moveToDirectory( targetParent ) );

        // then
        assertEquals( "This is the file contained in directory A", readFromFile( targetFileOne ) );
        assertEquals( "This is the file contained in directory B", readFromFile( targetFileTwo ) );
    }

    @Test
    public void filesAreMovedBeforeDirectories() throws IOException
    {
        // given there is a file contained in a directory
        File parentDirectory = testDirectory.cleanDirectory( "parent" );
        File sourceDirectory = new File( parentDirectory, "source" );
        assertTrue( sourceDirectory.mkdirs() );
        File childFile = new File( sourceDirectory, "child" );
        assertTrue( childFile.createNewFile() );
        writeToFile( childFile, "Content" );

        // and we have an expected target directory
        File targetDirectory = new File( parentDirectory, "target" );
        assertTrue( targetDirectory.mkdirs() );

        // when
        subject.traverseForMoving( sourceDirectory ).forEach( moveToDirectory( targetDirectory ) );

        // then no exception due to files happening before empty target directory
    }

    private Consumer<FileMoveAction> moveToDirectory( File toDirectory )
    {
        return fileMoveAction ->
        {
            try
            {
                fileMoveAction.move( toDirectory );
            }
            catch ( Throwable throwable )
            {
                throw new AssertionError( throwable );
            }
        };
    }

    private String readFromFile( File input ) throws IOException
    {
        BufferedReader fileReader = new BufferedReader( new FileReader( input ) );
        StringBuilder stringBuilder = new StringBuilder();
        char[] data = new char[32];
        int read;
        while ( (read = fileReader.read( data )) != -1 )
        {
            stringBuilder.append( data, 0, read );
        }
        return stringBuilder.toString();
    }

    private void writeToFile( File output, String input ) throws IOException
    {
        try ( BufferedWriter bw = new BufferedWriter( new FileWriter( output ) ) )
        {
            bw.write( input );
        }
    }
}
