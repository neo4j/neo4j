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

import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileMovePropagatorTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private FileMovePropagator subject;

    @Before
    public void setup()
    {
        subject = new FileMovePropagator();
    }

    @Test
    public void moveSingleFiles()
    {
        // given
        File sharedParent = testDirectory.directory( "shared_parent" );
        File sourceParent = createDirectory( new File( sharedParent, "source" ) );
        File sourceFile = createFile( new File( sourceParent, "file.txt" ) );
        writeToFile( sourceFile, "Garbage data" );
        File targetParent = createDirectory( new File( sharedParent, "target" ) );
        File targetFile = new File( targetParent, "file.txt" );

        // when
        subject.traverseGenerateMoveActions( sourceFile, sourceParent ).forEach( moveToDirectory( targetParent ));

        // then
        assertEquals( "Garbage data", readFromFile( targetFile ) );
    }

    private interface RunnableThrowable
    {
        void run() throws Throwable;
    }

    private static Runnable runnableFromThrowable( RunnableThrowable runnableThrowable )
    {
        return () ->
        {
            try
            {
                runnableThrowable.run();
            }
            catch ( Throwable throwable )
            {
                throw new RuntimeException( throwable );
            }
        };
    }

    @Test
    public void moveSingleEmptyDirectories()
    {
        // given
        File sharedParent = testDirectory.directory( "shared_parent" );
        File sourceParent = createDirectory( new File( sharedParent, "source" ) );
        File sourceDirectory = createDirectory( new File( sourceParent, "directory" ) );

        // and
        File targetParent = createDirectory( new File( sharedParent, "target" ) );
        File targetDirectory = new File( targetParent, "directory" );

        // when
        subject.traverseGenerateMoveActions( sourceParent, sourceParent ).forEach( moveToDirectory( targetParent ) );

        // then
        assertTrue( sourceDirectory.exists() );
        assertTrue( targetDirectory.exists() );
        assertTrue( targetDirectory.isDirectory() );
    }

    @Test
    public void moveNestedFiles()
    {
        // given
        File sharedParent = testDirectory.directory( "shared_parent" );
        File sourceParent = createDirectory( new File( sharedParent, "source" ) );
        File targetParent = createDirectory( new File( sharedParent, "target" ) );

        // and
        File nestedFileOne = createFile( new File( createDirectory( new File( sourceParent, "A" ) ), "file.txt" ) );
        File nestedFileTwo = createFile( new File( createDirectory( new File( sourceParent, "B" ) ), "file.txt" ) );
        writeToFile( nestedFileOne, "This is the file contained in directory A" );
        writeToFile( nestedFileTwo, "This is the file contained in directory B" );

        // and
        File targetFileOne = new File( targetParent, "A/file.txt" );
        File targetFileTwo = new File( targetParent, "B/file.txt" );

        // when
        subject.traverseGenerateMoveActions( sourceParent, sourceParent ).forEach( moveToDirectory( targetParent ) );

        // then
        assertEquals( "This is the file contained in directory A", readFromFile( targetFileOne ) );
        assertEquals( "This is the file contained in directory B", readFromFile( targetFileTwo ) );
    }

    @Test
    public void TODOtestPageCacheMoves() {} // TODO

    private Consumer<FileMoveAction> moveToDirectory( File directory )
    {
        return fileMoveAction -> runnableFromThrowable( () -> fileMoveAction.move( directory ) ).run();
    }

    private String readFromFile( File input )
    {
        try
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
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private File createDirectory( File file )
    {
        runnableFromThrowable( () -> file.mkdirs() ).run();
        return file;
    }

    private File createFile( File file )
    {
        runnableFromThrowable( () -> file.createNewFile() ).run();
        return file;
    }

    private void writeToFile( File output, String input )
    {
        try
        {
            BufferedWriter bw = new BufferedWriter( new FileWriter( output ) );
            bw.write( input );
            bw.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
