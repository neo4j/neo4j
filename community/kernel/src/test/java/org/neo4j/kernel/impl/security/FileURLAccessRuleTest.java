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
package org.neo4j.kernel.impl.security;

import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.kernel.configuration.Config;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class FileURLAccessRuleTest
{
    @Test
    public void shouldThrowWhenFileURLContainsAuthority() throws Exception
    {
        try
        {
            URLAccessRules.fileAccess().validate( Config.defaults(), new URL( "file://foo/bar/baz" ) );
            fail( "expected exception not thrown " );
        }
        catch ( URLAccessValidationError error )
        {
            assertThat( error.getMessage(),
                    equalTo( "file URL may not contain an authority section (i.e. it should be 'file:///')" ) );
        }
    }

    @Test
    public void shouldThrowWhenFileURLContainsQuery() throws Exception
    {
        try
        {
            URLAccessRules.fileAccess().validate( Config.defaults(), new URL( "file:///bar/baz?q=foo" ) );
            fail( "expected exception not thrown " );
        }
        catch ( URLAccessValidationError error )
        {
            assertThat( error.getMessage(), equalTo( "file URL may not contain a query component" ) );
        }
    }

    @Test
    public void shouldThrowWhenFileAccessIsDisabled() throws Exception
    {
        final URL url = new URL( "file:///bar/baz.csv" );
        final Config config = Config.defaults( GraphDatabaseSettings.allow_file_urls, "false" );
        try
        {
            URLAccessRules.fileAccess().validate( config, url );
            fail( "expected exception not thrown " );
        }
        catch ( URLAccessValidationError error )
        {
            assertThat( error.getMessage(), equalTo( "configuration property 'dbms.security.allow_csv_import_from_file_urls' is false" ) );
        }
    }

    @Test
    public void shouldThrowWhenRelativePathIsOutsideImportDirectory() throws Exception
    {
        assumeFalse( Paths.get( "/" ).relativize( Paths.get( "/../baz.csv" ) ).toString().equals( "baz.csv" ) );
        File importDir = new File( "/tmp/neo4jtest" ).getAbsoluteFile();
        final Config config = Config.defaults( GraphDatabaseSettings.load_csv_file_url_root, importDir.toString() );
        try
        {
            URLAccessRules.fileAccess().validate( config, new URL( "file:///../baz.csv" ) );
            fail( "expected exception not thrown " );
        }
        catch ( URLAccessValidationError error )
        {
            assertThat( error.getMessage(), equalTo( "file URL points outside configured import directory" ) );
        }
    }

    @Test
    public void shouldAdjustURLToWithinImportDirectory() throws Exception
    {
        final URL url = new File( "/bar/baz.csv" ).toURI().toURL();
        final Config config = Config.defaults( GraphDatabaseSettings.load_csv_file_url_root, "/var/lib/neo4j/import" );
        URL accessURL = URLAccessRules.fileAccess().validate( config, url );
        URL expected = new File( "/var/lib/neo4j/import/bar/baz.csv" ).toURI().toURL();
        assertEquals( expected, accessURL );
    }
}
