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
package org.neo4j.io.fs;

import org.apache.commons.lang3.SystemUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static org.neo4j.function.Predicates.alwaysTrue;

/**
 * Set of utility methods to work with {@link File} and {@link Path} using the {@link DefaultFileSystemAbstraction default file system}.
 * This class is used by {@link DefaultFileSystemAbstraction} and its methods should not take {@link FileSystemAbstraction} as a parameter.
 * Consider using {@link FileSystemUtils} when a helper method needs to work with different file systems.
 *
 * @see FileSystemUtils
 */
public final class FileUtils
{
    private static final int NUMBER_OF_RETRIES = 5;

    private FileUtils()
    {
        throw new AssertionError();
    }

    public static void deleteRecursively( File directory ) throws IOException
    {
        if ( !directory.exists() )
        {
            return;
        }
        Path path = directory.toPath();
        deletePathRecursively( path );
    }

    public static void deletePathRecursively( Path path ) throws IOException
    {
        deletePathRecursively( path, alwaysTrue() );
    }

    public static long blockSize( File file ) throws IOException
    {
        requireNonNull( file );
        var path = file.toPath();
        while ( path != null && !Files.exists( path ) )
        {
            path = path.getParent();
        }
        if ( path == null )
        {
            throw new IOException( "Fail to determine block size for file: " + file );
        }
        return Files.getFileStore( path ).getBlockSize();
    }

    public static void deletePathRecursively( Path path, Predicate<Path> removeFilePredicate ) throws IOException
    {

        windowsSafeIOOperation( () ->
        {
            Files.walkFileTree( path, new SimpleFileVisitor<>()
            {
                private int skippedFiles;

                @Override
                public FileVisitResult visitFile( Path file, BasicFileAttributes attrs ) throws IOException
                {
                    if ( removeFilePredicate.test( file ) )
                    {
                        Files.delete( file );
                    }
                    else
                    {
                        skippedFiles++;
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory( Path dir, IOException e ) throws IOException
                {
                    if ( e != null )
                    {
                        throw e;
                    }
                    try
                    {
                        if ( skippedFiles == 0 )
                        {
                            Files.delete( dir );
                            return FileVisitResult.CONTINUE;
                        }
                        if ( isDirectoryEmpty( dir ) )
                        {
                            Files.delete( dir );
                        }
                        return FileVisitResult.CONTINUE;
                    }
                    catch ( DirectoryNotEmptyException notEmpty )
                    {
                        String reason = notEmptyReason( dir, notEmpty );
                        throw new IOException( notEmpty.getMessage() + ": " + reason, notEmpty );
                    }
                }

                private boolean isDirectoryEmpty( Path dir ) throws IOException
                {
                    try ( Stream<Path> list = Files.list( dir ) )
                    {
                        return list.noneMatch( alwaysTrue() );
                    }
                }

                private String notEmptyReason( Path dir, DirectoryNotEmptyException notEmpty )
                {
                    try ( Stream<Path> list = Files.list( dir ) )
                    {
                        return list.map( p -> String.valueOf( p.getFileName() ) ).collect( Collectors.joining( "', '", "'", "'." ) );
                    }
                    catch ( Exception e )
                    {
                        notEmpty.addSuppressed( e );
                        return "(could not list directory: " + e.getMessage() + ')';
                    }
                }
            } );
        } );
    }

    public static boolean deleteFile( File file )
    {
        if ( !file.exists() )
        {
            return true;
        }
        else if ( file.isDirectory() )
        {
            File[] files = file.listFiles();
            if ( files == null )
            {
                // should typically mean IOException because we already confirmed existence, however code is not thread-safe
                return false;
            }
            else if ( files.length > 0 )
            {
                return false;
            }
        }
        int count = 0;
        boolean deleted;
        do
        {
            deleted = file.delete();
            if ( !deleted )
            {
                count++;
                waitAndThenTriggerGC();
            }
        }
        while ( !deleted && count <= NUMBER_OF_RETRIES );
        return deleted;
    }

    /**
     * Utility method that moves a file from its current location to the
     * new target location. If rename fails (for example if the target is
     * another disk) a copy/delete will be performed instead. This is not a rename,
     * use {@link #renameFile(File, File, CopyOption...)} instead.
     *
     * @param toMove The File object to move.
     * @param target Target file to move to.
     * @throws IOException if an IO error occurs.
     */
    public static void moveFile( File toMove, File target ) throws IOException
    {
        if ( !toMove.exists() )
        {
            throw new FileNotFoundException( "Source file[" + toMove.getAbsolutePath()
                                             + "] not found" );
        }
        if ( target.exists() )
        {
            throw new IOException( "Target file[" + target.getAbsolutePath()
                                   + "] already exists" );
        }

        if ( toMove.renameTo( target ) )
        {
            return;
        }

        if ( toMove.isDirectory() )
        {
            Files.createDirectories( target.toPath() );
            copyRecursively( toMove, target );
            deleteRecursively( toMove );
        }
        else
        {
            copyFile( toMove, target );
            deleteFile( toMove );
        }
    }

    /**
     * Utility method that moves a file from its current location to the
     * provided target directory. If rename fails (for example if the target is
     * another disk) a copy/delete will be performed instead. This is not a rename,
     * use {@link #renameFile(File, File, CopyOption...)} instead.
     *
     * @param toMove The File object to move.
     * @param targetDirectory the destination directory
     * @return the new file, null iff the move was unsuccessful
     * @throws IOException if an IO error occurs.
     */
    public static File moveFileToDirectory( File toMove, File targetDirectory ) throws IOException
    {
        if ( !targetDirectory.isDirectory() )
        {
            throw new IllegalArgumentException(
                    "Move target must be a directory, not " + targetDirectory );
        }

        File target = new File( targetDirectory, toMove.getName() );
        moveFile( toMove, target );
        return target;
    }

    /**
     * Utility method that copy a file from its current location to the
     * provided target directory.
     *
     * @param file file that needs to be copied.
     * @param targetDirectory the destination directory
     * @throws IOException if an IO error occurs.
     */
    public static void copyFileToDirectory( File file, File targetDirectory ) throws IOException
    {
        if ( !targetDirectory.exists() )
        {
            Files.createDirectories( targetDirectory.toPath() );
        }
        if ( !targetDirectory.isDirectory() )
        {
            throw new IllegalArgumentException(
                    "Move target must be a directory, not " + targetDirectory );
        }

        File target = new File( targetDirectory, file.getName() );
        copyFile( file, target );
    }

    public static void renameFile( File srcFile, File renameToFile, CopyOption... copyOptions ) throws IOException
    {
        Files.move( srcFile.toPath(), renameToFile.toPath(), copyOptions );
    }

    public static void truncateFile( SeekableByteChannel fileChannel, long position ) throws IOException
    {
        windowsSafeIOOperation( () -> fileChannel.truncate( position ) );
    }

    public static void truncateFile( File file, long position ) throws IOException
    {
        try ( RandomAccessFile access = new RandomAccessFile( file, "rw" ) )
        {
            truncateFile( access.getChannel(), position );
        }
    }

    /*
     * See http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154.
     */
    private static void waitAndThenTriggerGC()
    {
        try
        {
            Thread.sleep( 500 );
        }
        catch ( InterruptedException ignored )
        {
        } // ok
        System.gc();
    }

    public static String fixSeparatorsInPath( String path )
    {
        String fileSeparator = System.getProperty( "file.separator" );
        if ( "\\".equals( fileSeparator ) )
        {
            path = path.replace( '/', '\\' );
        }
        else if ( "/".equals( fileSeparator ) )
        {
            path = path.replace( '\\', '/' );
        }
        return path;
    }

    public static void copyFile( File srcFile, File dstFile ) throws IOException
    {
        copyFile( srcFile, dstFile, StandardCopyOption.REPLACE_EXISTING );
    }

    public static void copyFile( File srcFile, File dstFile, CopyOption... copyOptions ) throws IOException
    {
        //noinspection ResultOfMethodCallIgnored
        dstFile.getParentFile().mkdirs();
        Files.copy( srcFile.toPath(), dstFile.toPath(), copyOptions );
    }

    public static void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        copyRecursively( fromDirectory, toDirectory, null );
    }

    public static void copyRecursively( File fromDirectory, File toDirectory, FileFilter filter ) throws IOException
    {
        File[] files = fromDirectory.listFiles( filter );
        if ( files != null )
        {
            for ( File fromFile : files )
            {
                File toFile = new File( toDirectory, fromFile.getName() );
                if ( fromFile.isDirectory() )
                {
                    Files.createDirectories( toFile.toPath() );
                    copyRecursively( fromFile, toFile, filter );
                }
                else
                {
                    copyFile( fromFile, toFile );
                }
            }
        }
    }

    public static void writeToFile( File target, String text, boolean append ) throws IOException
    {
        if ( !target.exists() )
        {
            Files.createDirectories( target.getParentFile().toPath() );
            //noinspection ResultOfMethodCallIgnored
            target.createNewFile();
        }

        try ( Writer out = new OutputStreamWriter( new FileOutputStream( target, append ), StandardCharsets.UTF_8 ) )
        {
            out.write( text );
        }
    }

    public static PrintWriter newFilePrintWriter( File file, Charset charset ) throws FileNotFoundException
    {
        return new PrintWriter( new OutputStreamWriter( new FileOutputStream( file, true ), charset ) );
    }

    public static File path( String root, String... path )
    {
        return path( new File( root ), path );
    }

    public static File path( File root, String... path )
    {
        for ( String part : path )
        {
            root = new File( root, part );
        }
        return root;
    }

    /**
     * Attempts to discern if the given path is mounted on a device that can likely sustain a very high IO throughput.
     * <p>
     * A high IO device is expected to have negligible seek time, if any, and be able to service multiple IO requests
     * in parallel.
     *
     * @param pathOnDevice Any path, hypothetical or real, that once fully resolved, would exist on a storage device
     * that either supports high IO, or not.
     * @return Our best-effort estimate for whether or not this device supports a high IO workload.
     */
    public static boolean highIODevice( Path pathOnDevice )
    {
       if ( SystemUtils.IS_OS_LINUX )
        {
            try
            {
                FileStore fileStore = Files.getFileStore( pathOnDevice );
                String name = fileStore.name();
                if ( name.equals( "tmpfs" ) || name.equals( "hugetlbfs" ) )
                {
                    // This is a purely in-memory device. It doesn't get faster than this.
                    return true;
                }

                if ( name.startsWith( "/dev/nvme" ) )
                {
                    // This is probably an NVMe device. Anything on that protocol is most likely very fast.
                    return true;
                }

                Path device = Paths.get( name ).toRealPath(); // Use toRealPath to resolve any symlinks.
                Path deviceName = device.getName( device.getNameCount() - 1 );

                Path rotational = rotationalPathFor( deviceName );
                if ( Files.exists( rotational ) )
                {
                    return readFirstCharacter( rotational ) == '0';
                }
                else
                {
                    String namePart = deviceName.toString();
                    int len = namePart.length();
                    while ( Character.isDigit( namePart.charAt( len - 1 ) ) )
                    {
                        len--;
                    }
                    deviceName = Paths.get( namePart.substring( 0, len ) );
                    rotational = rotationalPathFor( deviceName );
                    if ( Files.exists( rotational ) )
                    {
                        return readFirstCharacter( rotational ) == '0';
                    }
                }
            }
            catch ( Exception ignored )
            {
                // This Linux system apparently has an unfamiliar storage configuration.
                // Let's just assume that whatever is going on here, it's probably fast.
                return true;
            }
        }

        // Most systems that are not running Linux, will be either MacOS or Windows, and those are likely to be laptops.
        // Nearly all modern laptops have SSDs as their primary storage, and in any case won't be doing performance critical work.
        // So we just assume that all non-Linux systems have high IO.
        return true;
    }

    private static Path rotationalPathFor( Path deviceName )
    {
        return Paths.get( "/sys/block" ).resolve( deviceName ).resolve( "queue" ).resolve( "rotational" );
    }

    private static int readFirstCharacter( Path file ) throws IOException
    {
        try ( InputStream in = Files.newInputStream( file, StandardOpenOption.READ ) )
        {
            return in.read();
        }
    }

    /**
     * Useful when you want to move a file from one directory to another by renaming the file
     * and keep eventual sub directories. Example:
     * <p>
     * You want to move file /a/b1/c/d/file from /a/b1 to /a/b2 and keep the sub path /c/d/file.
     * <pre>
     * <code>fileToMove = new File( "/a/b1/c/d/file" );
     * fromDir = new File( "/a/b1" );
     * toDir = new File( "/a/b2" );
     * fileToMove.rename( pathToFileAfterMove( fromDir, toDir, fileToMove ) );
     * // fileToMove.getAbsolutePath() -> /a/b2/c/d/file</code>
     * </pre>
     * Calls {@link #pathToFileAfterMove(Path, Path, Path)} after
     * transforming given files to paths by calling {@link File#toPath()}.
     * <p>
     * NOTE: This that this does not perform the move, it only calculates the new file name.
     * <p>
     * Throws {@link IllegalArgumentException} is fileToMove is not a sub path to fromDir.
     *
     * @param fromDir Current parent directory for fileToMove
     * @param toDir Directory denoting new parent directory for fileToMove after move
     * @param fileToMove File denoting current location for fileToMove
     * @return {@link File} denoting new abstract path for file after move.
     */
    public static File pathToFileAfterMove( File fromDir, File toDir, File fileToMove )
    {
        final Path fromDirPath = fromDir.toPath();
        final Path toDirPath = toDir.toPath();
        final Path fileToMovePath = fileToMove.toPath();
        return pathToFileAfterMove( fromDirPath, toDirPath, fileToMovePath ).toFile();
    }

    /**
     * Resolve toDir against fileToMove relativized against fromDir, resulting in a path denoting the location of
     * fileToMove after being moved fromDir toDir.
     * <p>
     * NOTE: This that this does not perform the move, it only calculates the new file name.
     * <p>
     * Throws {@link IllegalArgumentException} is fileToMove is not a sub path to fromDir.
     *
     * @param fromDir Path denoting current parent directory for fileToMove
     * @param toDir Path denoting location for fileToMove after move
     * @param fileToMove Path denoting current location for fileToMove
     * @return {@link Path} denoting new abstract path for file after move.
     */
    public static Path pathToFileAfterMove( Path fromDir, Path toDir, Path fileToMove )
    {
        // File to move must be true sub path to from dir
        if ( !fileToMove.startsWith( fromDir ) || fileToMove.equals( fromDir ) )
        {
            throw new IllegalArgumentException( "File " + fileToMove + " is not a sub path to dir " + fromDir );
        }

        return toDir.resolve( fromDir.relativize( fileToMove ) );
    }

    /**
     * Count the number of files and directories, contained in the given {@link Path}, which must be a directory.
     * @param dir The directory whose contents to count.
     * @return The number of files and directories in the given directory.
     * @throws NotDirectoryException If the given {@link Path} is not a directory. This exception is an optionally
     * specific exception. {@link IOException} might be thrown instead.
     * @throws IOException If the given directory could not be opened for some reason.
     */
    public static long countFilesInDirectoryPath( Path dir ) throws IOException
    {
        try ( Stream<Path> listing = Files.list( dir ) )
        {
            return listing.count();
        }
    }

    public interface Operation
    {
        void perform() throws IOException;
    }

    public static void windowsSafeIOOperation( Operation operation ) throws IOException
    {
        IOException storedIoe = null;
        for ( int i = 0; i < NUMBER_OF_RETRIES; i++ )
        {
            try
            {
                operation.perform();
                return;
            }
            catch ( IOException e )
            {
                storedIoe = e;
                waitAndThenTriggerGC();
            }
        }
        throw requireNonNull( storedIoe );
    }

    /**
     * Given a directory and a path under it, return filename of the path
     * relative to the directory.
     *
     * @param baseDir The base directory, containing the storeFile
     * @param storeFile The store file path, must be contained under
     * <code>baseDir</code>
     * @return The relative path of <code>storeFile</code> to
     * <code>baseDir</code>
     * @throws IOException As per {@link File#getCanonicalPath()}
     */
    public static String relativePath( File baseDir, File storeFile )
            throws IOException
    {
        String prefix = baseDir.getCanonicalPath();
        String path = storeFile.getCanonicalPath();
        if ( !path.startsWith( prefix ) )
        {
            throw new FileNotFoundException();
        }
        path = path.substring( prefix.length() );
        if ( path.startsWith( File.separator ) )
        {
            return path.substring( 1 );
        }
        return path;
    }

    /**
     * Canonical file resolution on windows does not resolve links.
     * Real paths on windows can be resolved only using {@link Path#toRealPath(LinkOption...)}, but file should exist in that case.
     * We will try to do as much as possible and will try to use {@link Path#toRealPath(LinkOption...)} when file exist and will fallback to only
     * use {@link File#getCanonicalFile()} if file does not exist.
     * see JDK-8003887 for details
     * @param file - file to resolve canonical representation
     * @return canonical file representation.
     */
    public static File getCanonicalFile( File file )
    {
        try
        {
            File fileToResolve = file.exists() ? file.toPath().toRealPath().toFile() : file;
            return fileToResolve.getCanonicalFile();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public static void writeAll( FileChannel channel, ByteBuffer src, long position ) throws IOException
    {
        long filePosition = position;
        long expectedEndPosition = filePosition + src.limit() - src.position();
        int bytesWritten;
        while ( (filePosition += bytesWritten = channel.write( src, filePosition )) < expectedEndPosition )
        {
            if ( bytesWritten <= 0 )
            {
                throw new IOException( "Unable to write to disk, reported bytes written was " + bytesWritten );
            }
        }
    }

    public static void writeAll( FileChannel channel, ByteBuffer src ) throws IOException
    {
        long bytesToWrite = src.limit() - src.position();
        int bytesWritten;
        while ( (bytesToWrite -= bytesWritten = channel.write( src )) > 0 )
        {
            if ( bytesWritten <= 0 )
            {
                throw new IOException( "Unable to write to disk, reported bytes written was " + bytesWritten );
            }
        }
    }

    public static FileChannel open( Path path, Set<OpenOption> options ) throws IOException
    {
        return FileChannel.open( path, options );
    }

    public static InputStream openAsInputStream( Path path ) throws IOException
    {
        return Files.newInputStream( path, READ );
    }

    public static OutputStream openAsOutputStream( Path path, boolean append ) throws IOException
    {
        OpenOption[] options;
        if ( append )
        {
            options = new OpenOption[]{CREATE, WRITE, APPEND};
        }
        else
        {
            options = new OpenOption[]{CREATE, WRITE};
        }
        return Files.newOutputStream( path, options );
    }

    /**
     * Get type of file store where provided file is located.
     * @param file file to get file store type for.
     * @return name of file store or "Unknown file store type: " + exception message,
     *         in case if exception occur during file store type retrieval.
     */
    public static String getFileStoreType( File file )
    {
        try
        {
            return Files.getFileStore( file.toPath() ).type();
        }
        catch ( IOException e )
        {
            return "Unknown file store type: " + e.getMessage();
        }
    }

    public static void tryForceDirectory( File directory ) throws IOException
    {
        if ( !directory.exists() )
        {
            throw new NoSuchFileException( format( "The directory %s does not exist!", directory.getAbsolutePath() ) );
        }
        else if ( !directory.isDirectory() )
        {
            throw new IllegalArgumentException( format( "The path %s must refer to a directory!", directory.getAbsolutePath() ) );
        }

        if ( SystemUtils.IS_OS_WINDOWS )
        {
            // Windows doesn't allow us to open a FileChannel against a directory for reading, so we can't attempt to "fsync" there
            return;
        }

        // Attempts to fsync the directory, guaranting e.g. file creation/deletion/rename events are durable
        // See http://mail.openjdk.java.net/pipermail/nio-dev/2015-May/003140.html
        // See also https://github.com/apache/lucene-solr/commit/7bea628bf3961a10581833935e4c1b61ad708c5c
        FileChannel directoryChannel = FileChannel.open( directory.toPath(), singleton( READ ) );
        directoryChannel.force( true );
    }

}
