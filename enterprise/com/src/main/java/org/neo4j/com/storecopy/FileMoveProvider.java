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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.StoreType;

import static org.neo4j.helpers.collection.Iterables.asList;

public class FileMoveProvider
{

    private final FileMoveActionInformer fileMoveActionInformer;
    private final PageCache pageCache;

    public FileMoveProvider( PageCache pageCache )
    {
        this( pageCache, StoreType::shouldBeManagedByPageCache );
    }

    public FileMoveProvider( PageCache pageCache, FileMoveActionInformer fileMoveActionInformer )
    {
        this.pageCache = pageCache;
        this.fileMoveActionInformer = fileMoveActionInformer;
    }

    /**
     * Construct a stream of files that are to be moved
     *
     * @param dir the source location of the move action
     * @return a stream of the entire contents of the source location that can be applied to a target location to perform a move
     */
    public Stream<FileMoveAction> traverseGenerateMoveActions( File dir )
    {
        return traverseGenerateMoveActions( dir, dir );
    }

    /**
     * Copies <b>the contents</b> from the directory to the base target path.
     * <p>
     * This is confusing, so here is an example
     * <p>
     * <p>
     * <code>
     * +Parent<br>
     * |+--directoryA<br>
     * |...+--fileA<br>
     * |...+--fileB<br>
     * </code>
     * <p>
     * Suppose we want to move to move <b>Parent/directoryA</b> to <b>Parent/directoryB</b>.<br>
     * <p>
     * <code>
     * File directoryA = new File("Parent/directoryA");<br>
     * Stream<FileMoveAction> fileMoveActions = new FileMoveProvider(pageCache).traverseGenerateMoveActions(directoryA, directoryA);<br>
     * </code>
     * </p>
     * In the above we clearly generate actions for moving all the files contained in directoryA. directoryA is mentioned twice due to a implementation detail,
     * hence the public method with only one parameter. We then actually perform the moves by applying the base target directory that we want to move to.
     * <p>
     * <code>
     * File directoryB = new File("Parent/directoryB");<br>
     * fileMoveActions.forEach( action -> action.move( directoryB ) );
     * </code>
     * </p>
     *
     * @param dir this directory and all the child paths under it are subject to move
     * @param basePath this is the parent of your intended target directory.
     * @return a stream of individual move actions which can be iterated and applied whenever
     */
    Stream<FileMoveAction> traverseGenerateMoveActions( File dir, File basePath )
    {
        // Note that flatMap is an *intermediate operation* and therefor always lazy.
        // It is very important that the stream we return only *lazily* calls out to expandTraverseFiles!
        return Stream.of( dir ).flatMap( d -> expandTraverseFiles( d, basePath ) );
    }

    private Stream<FileMoveAction> expandTraverseFiles( File dir, File basePath )
    {
        List<File> listing = listFiles( dir );
        if ( listing.isEmpty() )
        {
//            return Stream.of( copyFileCorrectly( dir, basePath ) );
            return Stream.empty();
        }
        Stream<File> files = listing.stream().filter( this::isFile );
        Stream<File> dirs = listing.stream().filter( this::isDirectory );
        Stream<FileMoveAction> moveFiles = files.map( f -> copyFileCorrectly( f, basePath ) );
        Stream<FileMoveAction> traverseDirectories = dirs.flatMap( d -> traverseGenerateMoveActions( d, basePath ) );
        return Stream.concat( moveFiles, traverseDirectories );
    }

    private boolean isFile( File file )
    {
        if ( fileMoveActionInformer.shouldBeManagedByPageCache( file.getName() ) )
        {
            return !pageCache.getCachedFileSystem().isDirectory( file );
        }
        return file.isFile();
    }

    private boolean isDirectory( File file )
    {
        if ( fileMoveActionInformer.shouldBeManagedByPageCache( file.getName() ) )
        {
            return pageCache.getCachedFileSystem().isDirectory( file );
        }
        return file.isDirectory();
    }

    private List<File> listFiles( File dir )
    {
        List<File> pageCacheFiles = asList( safeArray( pageCache.getCachedFileSystem().listFiles( dir ) ) );
        List<File> fsFiles = safeArray( dir.listFiles() );
        return Stream.of( pageCacheFiles, fsFiles ).flatMap( List::stream ).collect( Collectors.toList() );
    }

    private List<File> safeArray( File[] files )
    {
        return Arrays.asList( Optional.ofNullable( files ).orElse( new File[]{} ) );
    }

    /**
     * Some files are handled via page cache for CAPI flash, others are only used on the default file system. This contains the logic for handling files between
     * the 2 systems
     */
    private FileMoveAction copyFileCorrectly( File fileToMove, File basePath )
    {
        if ( fileMoveActionInformer.shouldBeManagedByPageCache( fileToMove.getName() ) )
        {
            return FileMoveAction.copyViaPageCache( fileToMove, pageCache );
        }
        return FileMoveAction.copyViaFileSystem( fileToMove, basePath );
    }
}
