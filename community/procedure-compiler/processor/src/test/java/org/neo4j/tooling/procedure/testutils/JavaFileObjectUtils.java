/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.tooling.procedure.testutils;

import com.google.testing.compile.JavaFileObjects;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import javax.tools.JavaFileObject;

import static org.assertj.core.api.Assertions.assertThat;

public enum JavaFileObjectUtils
{
    INSTANCE;

    private final String baseDirectory;

    JavaFileObjectUtils()
    {
        Properties properties = loadProperties( "/procedures.properties" );
        baseDirectory = properties.getProperty( "base_directory" );
        assertThat( new File( baseDirectory ) ).exists();
    }

    public JavaFileObject procedureSource( String relativePath )
    {
        return JavaFileObjects.forResource( resolveUrl( relativePath ) );
    }

    private final Properties loadProperties( String name )
    {
        try ( InputStream paths = this.getClass().getResourceAsStream( name ) )
        {
            Properties properties = new Properties();
            properties.load( paths );
            return properties;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private URL resolveUrl( String relativePath )
    {
        try
        {
            return new File( baseDirectory, relativePath ).toURI().toURL();
        }
        catch ( MalformedURLException e )
        {
            throw new RuntimeException( e.getMessage(), e );
        }
    }
}
