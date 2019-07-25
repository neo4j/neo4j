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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.security.URLAccessValidationError;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class FileURLAccessRuleTest
{
    @Test
    void shouldThrowWhenFileURLContainsAuthority()
    {
        var error = assertThrows( URLAccessValidationError.class, () ->
            URLAccessRules.fileAccess().validate( Config.defaults(), new URL( "file://foo/bar/baz" ) ) );
        assertThat( error.getMessage(), equalTo( "file URL may not contain an authority section (i.e. it should be 'file:///')" ) );
    }

    @Test
    void shouldThrowWhenFileURLContainsQuery()
    {
        var error = assertThrows( URLAccessValidationError.class, () ->
            URLAccessRules.fileAccess().validate( Config.defaults(), new URL( "file:///bar/baz?q=foo" ) ) );
        assertThat( error.getMessage(), equalTo( "file URL may not contain a query component" ) );
    }

    @Test
    void shouldThrowWhenFileAccessIsDisabled() throws Exception
    {
        final URL url = new URL( "file:///bar/baz.csv" );
        final Config config = Config.defaults( GraphDatabaseSettings.allow_file_urls, false );
        var error = assertThrows( URLAccessValidationError.class, () ->
            URLAccessRules.fileAccess().validate( config, url ) );
        assertThat( error.getMessage(), equalTo( "configuration property 'dbms.security.allow_csv_import_from_file_urls' is false" ) );
    }

    @Test
    void shouldThrowWhenRelativePathIsOutsideImportDirectory()
    {
        assumeFalse( Paths.get( "/" ).relativize( Paths.get( "/../baz.csv" ) ).toString().equals( "baz.csv" ) );
        File importDir = new File( "/tmp/neo4jtest" ).getAbsoluteFile();
        final Config config = Config.defaults( GraphDatabaseSettings.load_csv_file_url_root, importDir.toPath() );
        var error = assertThrows( URLAccessValidationError.class, () ->
            URLAccessRules.fileAccess().validate( config, new URL( "file:///../baz.csv" ) ) );
        assertThat( error.getMessage(), equalTo( "file URL points outside configured import directory" ) );
    }

    @Test
    void shouldAdjustURLToWithinImportDirectory() throws Exception
    {
        final URL url = new File( "/bar/baz.csv" ).toURI().toURL();
        final Config config = Config.defaults( GraphDatabaseSettings.load_csv_file_url_root, Path.of( "/var/lib/neo4j/import" ) );
        URL accessURL = URLAccessRules.fileAccess().validate( config, url );
        URL expected = new File( "/var/lib/neo4j/import/bar/baz.csv" ).toURI().toURL();
        assertEquals( expected, accessURL );
    }
}
