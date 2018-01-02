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
package org.neo4j.kernel.impl.security;

import java.io.File;
import java.net.URL;

import org.junit.Test;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.security.URLAccessValidationError;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileURLAccessRuleTest
{
    @Test
    public void shouldThrowWhenFileURLContainsAuthority() throws Exception
    {
        try
        {
            URLAccessRules.fileAccess().validate( mock( GraphDatabaseAPI.class ), new URL( "file://foo/bar/baz" ) );
            fail( "expected exception not thrown " );
        }
        catch ( URLAccessValidationError error )
        {
            assertThat( error.getMessage(), equalTo( "file URL may not contain an authority section (i.e. it should be 'file:///')" ) );
        }
    }

    @Test
    public void shouldThrowWhenFileURLContainsQuery() throws Exception
    {
        try
        {
            URLAccessRules.fileAccess().validate( mock( GraphDatabaseAPI.class ), new URL( "file:///bar/baz?q=foo" ) );
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
        final GraphDatabaseAPI gdb = mock( GraphDatabaseAPI.class );
        final DependencyResolver mockResolver = mock( DependencyResolver.class );
        when( gdb.getDependencyResolver() ).thenReturn( mockResolver );
        final Config config = new Config( MapUtil.stringMap( GraphDatabaseSettings.allow_file_urls.name(), "false" ) );
        when( mockResolver.resolveDependency( eq( Config.class ) ) ).thenReturn( config );

        try
        {
            URLAccessRules.fileAccess().validate( gdb, url );
            fail( "expected exception not thrown " );
        }
        catch ( URLAccessValidationError error )
        {
            assertThat( error.getMessage(), equalTo( "configuration property 'allow_file_urls' is false" ) );
        }
    }

    @Test
    public void shouldThrowWhenRelativePathIsOutsideImportDirectory() throws Exception
    {
        File importDir = new File( "/tmp/neo4jtest" ).getAbsoluteFile();
        final Config config = new Config(
                MapUtil.stringMap( GraphDatabaseSettings.load_csv_file_url_root.name(), importDir.toString() ) );
        final GraphDatabaseAPI gdb = mock( GraphDatabaseAPI.class );
        final DependencyResolver mockResolver = mock( DependencyResolver.class );
        when( gdb.getDependencyResolver() ).thenReturn( mockResolver );
        when( mockResolver.resolveDependency( eq( Config.class ) ) ).thenReturn( config );
        try
        {
            URLAccessRules.fileAccess().validate( gdb, new URL( "file:///../baz.csv" ) );
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
        final GraphDatabaseAPI gdb = mock( GraphDatabaseAPI.class );
        final DependencyResolver mockResolver = mock( DependencyResolver.class );
        when( gdb.getDependencyResolver() ).thenReturn( mockResolver );
        final Config config = new Config( MapUtil.stringMap( GraphDatabaseSettings.load_csv_file_url_root.name(), "/var/lib/neo4j/import" ) );
        when( mockResolver.resolveDependency( eq( Config.class ) ) ).thenReturn( config );

        URL accessURL = URLAccessRules.fileAccess().validate( gdb, url );
        URL expected = new File( "/var/lib/neo4j/import/bar/baz.csv" ).toURI().toURL();
        assertEquals( expected, accessURL );
    }
}
