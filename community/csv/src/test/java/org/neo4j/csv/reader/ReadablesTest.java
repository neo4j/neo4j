/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.csv.reader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.TestDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ReadablesTest
{
    @Test
    public void shouldReadTextCompressedInZipArchiveWithSingleFileIn() throws Exception
    {
        // GIVEN
        String text = "abcdefghijlkmnopqrstuvxyz";
        File compressed = compressWithZip( text );

        // WHEN
        CharReadable readable = Readables.file( compressed );
        char[] readText = new char[text.toCharArray().length];
        readable.read( readText, 0, readText.length );

        // THEN
        assertArrayEquals( text.toCharArray(), readText );
    }

    @Test
    public void shouldReadTextCompressedInGZipFile() throws Exception
    {
        // GIVEN
        String text = "abcdefghijlkmnopqrstuvxyz";
        File compressed = compressWithGZip( text );

        // WHEN
        CharReadable readable = Readables.file( compressed );
        char[] readText = new char[text.toCharArray().length];
        readable.read( readText, 0, readText.length );

        // THEN
        assertArrayEquals( text.toCharArray(), readText );
    }

    @Test
    public void shouldReadPlainTextFile() throws Exception
    {
        // GIVEN
        String text = "abcdefghijlkmnopqrstuvxyz";
        File plainText = write( text );

        // WHEN
        CharReadable readable = Readables.file( plainText );
        char[] readText = new char[text.toCharArray().length];
        readable.read( readText, 0, readText.length );

        // THEN
        assertArrayEquals( text.toCharArray(), readText );
    }

    @Test
    public void shouldReadTheOnlyRealFileInThere() throws Exception
    {
        // GIVEN
        String text = "abcdefghijlkmnopqrstuvxyz";
        File compressed = compressWithZip( text, ".nothing", ".DS_Store", "__MACOSX/", "__MACOSX/file" );

        // WHEN
        CharReadable readable = Readables.file( compressed );
        char[] readText = new char[text.toCharArray().length];
        readable.read( readText, 0, readText.length );

        // THEN
        assertArrayEquals( text.toCharArray(), readText );
    }

    @Test
    public void shouldFailWhenThereAreMoreThanOneSuitableFileInThere() throws Exception
    {
        // GIVEN
        String text = "abcdefghijlkmnopqrstuvxyz";
        File compressed = compressWithZip( text, ".nothing", ".DS_Store", "somewhere/something" );

        // WHEN
        CharReadable readable;
        try
        {
            readable = Readables.file( compressed );
            fail( "Should fail since there are multiple suitable files in the zip archive" );
        }
        catch ( IOException e )
        {   // Good
            assertThat( e.getMessage(), containsString( "Multiple" ) );
        }
    }

    private File write( String text ) throws IOException
    {
        File file = directory.file( "plain-text" );
        try ( OutputStream out = new FileOutputStream( file ) )
        {
            out.write( text.getBytes() );
        }
        return file;
    }

    // TODO test for failing reading a ZIP archive with multiple files in

    private File compressWithZip( String text, String... otherEntries ) throws IOException
    {
        File file = directory.file( "compressed" );
        try ( ZipOutputStream out = new ZipOutputStream( new FileOutputStream( file ) ) )
        {
            for ( String otherEntry : otherEntries )
            {
                out.putNextEntry( new ZipEntry( otherEntry ) );
            }

            out.putNextEntry( new ZipEntry( "file" ) );
            out.write( text.getBytes() );
        }
        return file;
    }

    private File compressWithGZip( String text ) throws IOException
    {
        File file = directory.file( "compressed" );
        try ( GZIPOutputStream out = new GZIPOutputStream( new FileOutputStream( file ) ) )
        {
            out.write( text.getBytes() );
        }
        return file;
    }

    public final @Rule TestDirectory directory = new TestDirectory();
}
