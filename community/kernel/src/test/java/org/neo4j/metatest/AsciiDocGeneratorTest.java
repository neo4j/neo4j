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
package org.neo4j.metatest;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.test.AsciiDocGenerator;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AsciiDocGeneratorTest
{
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    private File sectionDirectory;

    @Before
    public void setup()
    {
        sectionDirectory = new File( testDirectory.directory( "testasciidocs" ), "testsection" );
    }

    @Test
    public void dumpToSeparateFile() throws IOException
    {
        String reference = AsciiDocGenerator.dumpToSeparateFile( sectionDirectory, "test1", ".title1\ntest1-content" );
        assertEquals( ".title1\ninclude::includes/test1.asciidoc[]\n", reference );
        File includeDir = new File( sectionDirectory, "includes" );
        File includeFile = new File( includeDir, "test1.asciidoc" );
        assertTrue( includeFile.canRead() );
        String fileContent = TestJavaTestDocsGenerator.readFileAsString( includeFile );
        assertEquals( "test1-content", fileContent );
    }

    @Test
    public void dumpToSeparateFileWithType() throws IOException
    {
        String reference = AsciiDocGenerator.dumpToSeparateFileWithType( sectionDirectory, "console", "test2-content" );
        assertEquals( "include::includes/console-1.asciidoc[]\n", reference );
        File includeDir = new File( sectionDirectory, "includes" );
        File includeFile = new File( includeDir, "console-1.asciidoc" );
        String fileContent = TestJavaTestDocsGenerator.readFileAsString( includeFile );
        assertEquals( "test2-content", fileContent );

        // make sure the next console doesn't overwrite the first one
        AsciiDocGenerator.dumpToSeparateFileWithType( sectionDirectory, "console", "test3-content" );
        includeFile = new File( includeDir, "console-2.asciidoc" );
        fileContent = TestJavaTestDocsGenerator.readFileAsString( includeFile );
        assertEquals( "test3-content", fileContent );
    }
}
