/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.neo4j.test.TestDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static java.util.Arrays.copyOfRange;

public class ReadablesTest
{
    @Test
    public void shouldReadTextCompressedInZipArchiveWithSingleFileIn() throws Exception
    {
        // GIVEN
        String text = "abcdefghijlkmnopqrstuvxyz";

        // WHEN
        File compressed = compressWithZip( text );

        // THEN
        assertReadText( compressed, text );
    }

    @Test
    public void shouldReadTextCompressedInGZipFile() throws Exception
    {
        // GIVEN
        String text = "abcdefghijlkmnopqrstuvxyz";

        // WHEN
        File compressed = compressWithGZip( text );

        // THEN
        assertReadText( compressed, text );
    }

    @Test
    public void shouldReadPlainTextFile() throws Exception
    {
        // GIVEN
        String text = "abcdefghijlkmnopqrstuvxyz";

        // WHEN
        File plainText = write( text );

        // THEN
        assertReadText( plainText, text );
    }

    @Test
    public void shouldReadTheOnlyRealFileInThere() throws Exception
    {
        // GIVEN
        String text = "abcdefghijlkmnopqrstuvxyz";

        // WHEN
        File compressed = compressWithZip( text, ".nothing", ".DS_Store", "__MACOSX/", "__MACOSX/file" );

        // THEN
        assertReadText( compressed, text );
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

    @Test
    public void shouldTrackPosition() throws Exception
    {
        // GIVEN
        String data = "1234567890";
        //                 ^   ^
        CharReadable reader = Readables.wrap( new StringReader( data ) );
        SectionedCharBuffer buffer = new SectionedCharBuffer( 4 );

        // WHEN
        int expected = 0;
        do
        {
            buffer = reader.read( buffer, buffer.front() );
            expected += buffer.available();

            // THEN
            assertEquals( expected, reader.position() );
        }
        while ( buffer.hasAvailable() );

        // and THEN
        assertEquals( data.toCharArray().length, expected );
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

    private void assertReadText( File file, String text ) throws IOException
    {
        CharReadable readable = Readables.file( file );
        SectionedCharBuffer readText = new SectionedCharBuffer( text.toCharArray().length );
        readable.read( readText, readText.front() );
        assertArrayEquals( text.toCharArray(), copyOfRange( readText.array(), readText.pivot(), readText.front() ) );
    }

    public final @Rule TestDirectory directory = new TestDirectory();
}
