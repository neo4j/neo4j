/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index.storage;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.impl.index.storage.layout.FolderLayout;
import org.neo4j.kernel.api.impl.index.storage.layout.IndexFolderLayout;
import org.neo4j.kernel.impl.util.NumberAwareStringComparator;

import static java.util.stream.Collectors.toList;

/**
 * Utility class that manages directory structure for a partitioned lucene index.
 * It is aware of the {@link FileSystemAbstraction file system} structure of all index related folders, lucene
 * {@link Directory directories} and {@link FailureStorage failure storage}.
 */
public class PartitionedIndexStorage
{
    private static final Comparator<File> FILE_COMPARATOR =
            ( o1, o2 ) -> NumberAwareStringComparator.INSTANCE.compare( o1.getName(), o2.getName() );

    private final DirectoryFactory directoryFactory;
    private final FileSystemAbstraction fileSystem;
    private final boolean archiveFailed;
    private final FolderLayout folderLayout;
    private final FailureStorage failureStorage;

    public PartitionedIndexStorage( DirectoryFactory directoryFactory, FileSystemAbstraction fileSystem,
            File rootFolder, boolean archiveFailed )
    {
        this.fileSystem = fileSystem;
        this.archiveFailed = archiveFailed;
        this.folderLayout = new IndexFolderLayout( rootFolder );
        this.directoryFactory = directoryFactory;
        this.failureStorage = new FailureStorage( fileSystem, folderLayout );
    }

    /**
     * Opens a {@link Directory lucene directory} for the given folder.
     *
     * @param folder the folder that denotes a lucene directory.
     * @return the lucene directory denoted by the given folder.
     * @throws IOException if directory can't be opened.
     */
    public Directory openDirectory( File folder ) throws IOException
    {
        return directoryFactory.open( folder );
    }

    /**
     * Resolves a folder for the partition with the given index.
     *
     * @param partition the partition index.
     * @return the folder where partition's lucene directory should be located.
     */
    public File getPartitionFolder( int partition )
    {
        return folderLayout.getPartitionFolder( partition );
    }

    /**
     * Resolves root folder for the given index.
     *
     * @return the folder containing index partition folders.
     */
    public File getIndexFolder()
    {
        return folderLayout.getIndexFolder();
    }

    /**
     * Create a failure storage in the {@link #getIndexFolder() index folder}.
     *
     * @throws IOException if failure storage creation fails.
     * @see FailureStorage#reserveForIndex()
     */
    public void reserveIndexFailureStorage() throws IOException
    {
        failureStorage.reserveForIndex();
    }

    /**
     * Writes index failure into the failure storage.
     *
     * @param failure the cause of the index failure.
     * @throws IOException if writing to the failure storage file failed.
     * @see FailureStorage#storeIndexFailure(String)
     */
    public void storeIndexFailure( String failure ) throws IOException
    {
        failureStorage.storeIndexFailure( failure );
    }

    /**
     * Retrieves stored index failure.
     *
     * @return index failure as string or {@code null} if there is no failure.
     * @see FailureStorage#loadIndexFailure()
     */
    public String getStoredIndexFailure()
    {
        return failureStorage.loadIndexFailure();
    }

    /**
     * For the given {@link File folder} removes all nested folders from both {@link FileSystemAbstraction file system}
     * and {@link Directory lucene directories}.
     *
     * @param folder the folder to clean up.
     * @throws IOException if some removal operation fails.
     */
    public void prepareFolder( File folder ) throws IOException
    {
        cleanupFolder( folder, archiveFailed );
        fileSystem.mkdirs( folder );
    }

    /**
     * For the given {@link File folder} removes the folder itself and all nested folders from both
     * {@link FileSystemAbstraction file system} and {@link Directory lucene directories}.
     *
     * @param folder the folder to remove.
     * @throws IOException if some removal operation fails.
     */
    public void cleanupFolder( File folder ) throws IOException
    {
        cleanupFolder( folder, false );
    }

    private void cleanupFolder( File folder, boolean archiveFailed ) throws IOException
    {
        List<File> partitionFolders = listFolders( folder );
        if ( !partitionFolders.isEmpty() )
        {
            try ( ZipOutputStream zip = archiveFile( folder, archiveFailed ) )
            {
                byte[] buffer = null;
                if ( zip != null )
                {
                    buffer = new byte[4 * 1024];
                }
                for ( File partitionFolder : partitionFolders )
                {
                    cleanupLuceneDirectory( partitionFolder, zip, buffer );
                }
            }
        }
        fileSystem.deleteRecursively( folder );
    }

    private ZipOutputStream archiveFile( File folder, boolean archiveFailed ) throws IOException
    {
        ZipOutputStream zip = null;
        if ( archiveFailed )
        {
            File archiveFile = new File( folder.getParent(),
                    "archive-" + folder.getName() + "-" + System.currentTimeMillis() + ".zip" );
            zip = new ZipOutputStream( fileSystem.openAsOutputStream( archiveFile, false ) );
        }
        return zip;
    }

    /**
     * Opens all {@link Directory lucene directories} contained in the {@link #getIndexFolder() index folder}.
     *
     * @return the map from file system  {@link File directory} to the corresponding {@link Directory lucene directory}.
     * @throws IOException if opening of some lucene directory (via {@link DirectoryFactory#open(File)}) fails.
     */
    public Map<File,Directory> openIndexDirectories() throws IOException
    {
        Map<File,Directory> directories = new LinkedHashMap<>();
        try
        {
            for ( File dir : listFolders() )
            {
                directories.put( dir, directoryFactory.open( dir ) );
            }
        }
        catch ( IOException oe )
        {
            try
            {
                IOUtils.closeAll( directories.values() );
            }
            catch ( Exception ce )
            {
                oe.addSuppressed( ce );
            }
            throw oe;
        }
        return directories;
    }

    /**
     * List all folders in the {@link #getIndexFolder() index folder}.
     *
     * @return the list of index partition folders or {@link Collections#emptyList() empty list} if index folder is
     * empty.
     */
    public List<File> listFolders()
    {
        return listFolders( getIndexFolder() );
    }

    private List<File> listFolders( File rootFolder )
    {
        File[] files = fileSystem.listFiles( rootFolder );
        return files == null ? Collections.emptyList()
                             : Stream.of( files )
                               .filter( fileSystem::isDirectory )
                               .sorted( FILE_COMPARATOR )
                               .collect( toList() );

    }

    /**
     * Removes content of the lucene directory denoted by the given {@link File file}. This might seem unnecessary
     * since we cleanup the folder using {@link FileSystemAbstraction file system} but in fact for testing we often use
     * in-memory directories whose content can't be removed via the file system.
     * <p>
     * Uses {@link FileUtils#windowsSafeIOOperation(FileUtils.FileOperation)} underneath.
     *
     * @param folder the path to the directory to cleanup.
     * @param zip an optional zip output stream to archive files into.
     * @param buffer a byte buffer to use for copying bytes from the files into the archive.
     * @throws IOException if removal operation fails.
     */
    private void cleanupLuceneDirectory( File folder, ZipOutputStream zip, byte[] buffer ) throws IOException
    {
        try ( Directory dir = directoryFactory.open( folder ) )
        {
            String folderName = folder.getName() + "/";
            if ( zip != null )
            {
                zip.putNextEntry( new ZipEntry( folderName ) );
                zip.closeEntry();
            }
            String[] indexFiles = dir.listAll();
            for ( String indexFile : indexFiles )
            {
                if ( zip != null )
                {
                    zip.putNextEntry( new ZipEntry( folderName + indexFile ) );
                    try ( IndexInput input = dir.openInput( indexFile, IOContext.READ ) )
                    {
                        for ( long pos = 0, size = input.length(); pos < size; )
                        {
                            int read = Math.min( buffer.length, (int) (size - pos) );
                            input.readBytes( buffer, 0, read );
                            pos += read;
                            zip.write( buffer, 0, read );
                        }
                    }
                    zip.closeEntry();
                }
                FileUtils.windowsSafeIOOperation( () -> dir.deleteFile( indexFile ) );
            }
        }
    }
}
