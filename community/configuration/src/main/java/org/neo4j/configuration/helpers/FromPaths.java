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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FromPaths
{
    private final Set<Path> paths;

    public FromPaths( String value )
    {
        validate( value );
        this.paths = buildPaths( value );
    }

    public boolean isSingle()
    {
        return paths.size() == 1;
    }

    public Set<Path> paths( Optional<DatabaseNamePattern> folderPattern )
    {
        return folderPattern.map( this::getAllFilteredPaths )
                            .orElse( paths );
    }

    private Set<Path> getAllFilteredPaths( DatabaseNamePattern pattern )
    {
        return paths.stream().flatMap( this::getAllFolders )
                    .filter( path ->
                             {
                                 final var absolutePath = path.toAbsolutePath();
                                 final var lastPathOfPath = absolutePath.getName( absolutePath.getNameCount() - 1 );
                                 return pattern.matches( lastPathOfPath.toString() );
                             } ).collect( Collectors.toSet() );
    }

    private Stream<Path> getAllFolders( Path path )
    {
        try ( var paths = Files.walk( path, 1 ) )
        {
            return paths.collect( Collectors.toSet() ).stream();
        }
        catch ( Exception ex )
        {
            throw new IllegalArgumentException( "Can't get folder content of path " + path.toAbsolutePath(), ex );
        }
    }

    private Set<Path> buildPaths( String value )
    {
        final var tokens = value.split( "," );
        return Arrays.stream( tokens )
                     .map( String::trim )
                     .filter( t -> !t.isEmpty() )
                     .map( t -> Path.of( t ) )
                     .collect( Collectors.toSet() );
    }

    private void validate( String value )
    {
        Objects.requireNonNull( value, "The provided from parameter is empty." );

        if ( value.trim().isEmpty() )
        {
            throw new IllegalArgumentException( "The provided from parameter is empty." );
        }
    }
}
