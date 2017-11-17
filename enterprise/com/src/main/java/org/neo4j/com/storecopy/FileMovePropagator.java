/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com.storecopy;

import java.io.File;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

public class FileMovePropagator // TODO rename this garbage
{
    /**
     * Copies <b>the contents</b> from the directory to the base target path.
     *
     * This is confusing, best read the tests
     *
     * @param dir this directory and all the child paths under it are subject to move
     * @param basePath this is the parent of your intended target directory.
     * @return a stream of individual move actions which can be iterated and applied whenever
     */
    public Stream<FileMoveAction> traverseGenerateMoveActions( File dir, File basePath )
    {
        // Note that flatMap is an *intermediate operation* and therefor always lazy.
        // It is very important that the stream we return only *lazily* calls out to expandTraverseFiles!
        System.out.printf( "Moving from %s \n\twith relation to basepath=%s\n", dir, basePath );
        return Stream.of( dir ).flatMap( d -> expandTraverseFiles( d, basePath ) );
    }

    private Stream<FileMoveAction> expandTraverseFiles( File dir, File basePath )
    {
        File[] listing = dir.listFiles();
        if ( Optional.ofNullable( listing ).map( array -> array.length ).orElse( 0 ) == 0 )
        {
            return dir.isFile() ? Stream.of( FileMoveAction.copyViaFileSystem( dir, basePath ) ) :
                   Stream.of( FileMoveAction.copyViaFileSystem( dir, basePath ));
        }
        Stream<File> files = Arrays.stream( listing ).filter( File::isFile );
        Stream<File> dirs = Arrays.stream( listing ).filter( File::isDirectory );
        Stream<FileMoveAction> moveFiles = files.map( f -> FileMoveAction.copyViaFileSystem( f, basePath ) );
        Stream<FileMoveAction> traverseDirectories = dirs.flatMap( d -> traverseGenerateMoveActions( d, basePath ) );
        return Stream.concat( moveFiles, traverseDirectories );
    }
}
