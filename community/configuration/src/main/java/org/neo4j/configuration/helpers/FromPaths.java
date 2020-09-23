/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.configuration.helpers;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;

public class FromPaths
{
    private final Set<Path> paths;
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    public FromPaths( String value )
    {
        validateNotEmpty( value );
        this.paths = buildPaths( value );
    }

    public boolean isSingle()
    {
        return paths.size() == 1;
    }

    public Set<Path> getPaths()
    {
        return paths;
    }

    private Set<Path> getAllFolders( Path path )
    {
        return Arrays.stream( fs.listFiles( path ) )
                     .filter( Files::isDirectory )
                     .collect( Collectors.toSet() );
    }

    private Set<Path> buildPaths( String value )
    {
        final var tokens = value.split( "," );
        return Arrays.stream( tokens )
                     .map( String::trim )
                     .filter( t -> !t.isEmpty() )
                     .map( path -> new File( path ).getAbsoluteFile() ) // Path class can't be used because Path can't be created with regex for some
                     // file systems
                     .peek( file ->
                            {
                                validateParentPath( file ); //Path class can't contain regex in the subpath
                                validateLastSubPath( file );
                            } )
                     .flatMap( file -> getAndFilterPaths( file ).stream() )
                     .collect( Collectors.toSet() );
    }

    private Set<Path> getAndFilterPaths( File file )
    {
        final var parent = file.getParent(); //not null, protect by validateParentPath
        final var pattern = new DatabaseNamePattern( file.getName() );
        if ( !pattern.containsPattern() )
        {
            return Set.of( Path.of( file.toString() ) );
        }

        return getAllFolders( Path.of( parent ) ).stream()
                                                 .filter( path ->
                                                          {
                                                              final var name = path.getName( path.getNameCount() - 1 );
                                                              return pattern.matches( name.toString() );
                                                          } )
                                                 .collect( Collectors.toSet() );
    }

    private void validateParentPath( File file )
    {
        final var parentPath = file.getParent();
        if ( parentPath != null && !parentPath.trim().isEmpty() )
        {
            final var asterisks = StringUtils.countMatches( parentPath, '*' );
            final var questionMarks = StringUtils.countMatches( parentPath, '?' );
            if ( asterisks > 0 || questionMarks > 0 )
            {
                throw new IllegalArgumentException( file.getAbsolutePath() + " is illegal. Asterisks and question marks should be placed in the last subpath" );
            }
        }
        else
        {
            throw new IllegalArgumentException( "From path with value=" + file.getAbsolutePath() + " should not point to the root of the file system" );
        }
    }

    private void validateLastSubPath( File file )
    {
        if ( file.getParent() == null || Path.of( file.getParent() ).getNameCount() == 0 )
        {
            throw new IllegalArgumentException( "From path with value=" + file.getAbsolutePath() + " should not point to the root of the file system" );
        }

        final var lastSubPath = file.getName();
        try
        {
            new DatabaseNamePattern( lastSubPath );
        }
        catch ( IllegalArgumentException ex )
        {
            throw new IllegalArgumentException( "Last path of " + file.getAbsolutePath() + " is in illegal format.", ex );
        }
    }

    private void validateNotEmpty( String path )
    {
        Objects.requireNonNull( path, "The provided from parameter is empty." );

        if ( path.trim().isEmpty() )
        {
            throw new IllegalArgumentException( "The provided from parameter is empty." );
        }
    }
}
