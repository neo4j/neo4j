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
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.kernel.security.URLAccessValidationError;

class FileURLAccessRule implements URLAccessRule
{
    @Override
    public URL validate( GraphDatabaseAPI gdb, URL url ) throws URLAccessValidationError
    {
        if ( !( url.getAuthority() == null || url.getAuthority().equals("") ) )
        {
            throw new URLAccessValidationError( "file URL may not contain an authority section (i.e. it should be 'file:///')" );
        }

        if ( !( url.getQuery() == null || url.getQuery().equals("") ) )
        {
            throw new URLAccessValidationError( "file URL may not contain a query component" );
        }

        final Config config = gdb.getDependencyResolver().resolveDependency( Config.class );
        if ( !config.get( GraphDatabaseSettings.allow_file_urls ) )
        {
            throw new URLAccessValidationError( "configuration property '" + GraphDatabaseSettings.allow_file_urls.name() + "' is false" );
        }

        final File root = config.get( GraphDatabaseSettings.load_csv_file_url_root );
        if ( root == null )
        {
            return url;
        }

        try
        {
            final Path urlPath = Paths.get( url.toURI() );
            final Path rootPath = root.toPath().normalize().toAbsolutePath();
            // Normalize to prevent dirty tricks like '..'
            final Path result = rootPath.resolve( urlPath.getRoot().relativize( urlPath ) ).normalize()
                    .toAbsolutePath();

            if ( result.startsWith( rootPath ) )
            {
                return result.toUri().toURL();
            }
            throw new URLAccessValidationError( "file URL points outside configured import directory" );
        }
        catch ( MalformedURLException | URISyntaxException e )
        {
            // unreachable
            throw new RuntimeException( e );
        }
    }
}
