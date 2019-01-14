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
package org.neo4j.io.fs;

import org.apache.commons.lang3.SystemUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
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
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NotDirectoryException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.WRITE;

public class FileUtils
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
        Files.walkFileTree( path, new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult visitFile( Path file, BasicFileAttributes attrs ) throws IOException
            {
                deleteFile( file );
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory( Path dir, IOException e ) throws IOException
            {
                if ( e != null )
                {
                    throw e;
                }
                Files.delete( dir );
                return FileVisitResult.CONTINUE;
            }
        } );
    }

    public static boolean deleteFile( File file )
    {
        if ( !file.exists() )
        {
            return true;
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
     * @throws IOException
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
     * @throws IOException
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
     * @throws IOException
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
        catch ( InterruptedException ee )
        {
            Thread.interrupted();
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
        //noinspection ResultOfMethodCallIgnored
        dstFile.getParentFile().mkdirs();
        Files.copy( srcFile.toPath(), dstFile.toPath(), StandardCopyOption.REPLACE_EXISTING );
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

    public static BufferedReader newBufferedFileReader( File file, Charset charset ) throws FileNotFoundException
    {
        return new BufferedReader( new InputStreamReader( new FileInputStream( file ), charset ) );
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
     * @param defaultHunch The default hunch for whether the device supports high IO or not. This will be returned if
     * we otherwise have no clue about the nature of the storage device.
     * @return Our best-effort estimate for whether or not this device supports a high IO workload.
     */
    public static boolean highIODevice( Path pathOnDevice, boolean defaultHunch )
    {
        // This method has been manually tested and correctly identifies the high IO volumes on our test servers.
        if ( SystemUtils.IS_OS_MAC )
        {
            // Most macs have flash storage, so let's assume true for them.
            return true;
        }

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
            catch ( Exception e )
            {
                return defaultHunch;
            }
        }

        return defaultHunch;
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
        throw Objects.requireNonNull( storedIoe );
    }

    public interface LineListener
    {
        void line( String line );
    }

    public static LineListener echo( final PrintStream target )
    {
        return target::println;
    }

    public static void readTextFile( File file, LineListener listener ) throws IOException
    {
        try ( BufferedReader reader = new BufferedReader( new FileReader( file ) ) )
        {
            String line;
            while ( (line = reader.readLine()) != null )
            {
                listener.line( line );
            }
        }
    }

    public static String readTextFile( File file, Charset charset ) throws IOException
    {
        StringBuilder out = new StringBuilder();
        for ( String s : Files.readAllLines( file.toPath(), charset ) )
        {
            out.append( s ).append( "\n" );
        }
        return out.toString();
    }

    private static void deleteFile( Path path ) throws IOException
    {
        windowsSafeIOOperation( () -> Files.delete( path ) );
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

    public static OpenOption[] convertOpenMode( OpenMode mode )
    {
        OpenOption[] options;
        switch ( mode )
        {
        case READ:
            options = new OpenOption[]{READ};
            break;
        case READ_WRITE:
            options = new OpenOption[]{CREATE, READ, WRITE};
            break;
        case SYNC:
            options = new OpenOption[]{CREATE, READ, WRITE, SYNC};
            break;
        case DSYNC:
            options = new OpenOption[]{CREATE, READ, WRITE, DSYNC};
            break;
        default:
            throw new IllegalArgumentException( "Unsupported mode: " + mode );
        }
        return options;
    }

    public static FileChannel open( Path path, OpenMode openMode ) throws IOException
    {
        return FileChannel.open( path, convertOpenMode( openMode ) );
    }

    public static InputStream openAsInputStream( Path path ) throws IOException
    {
        return Files.newInputStream( path, READ );
    }

    /**
     * Check if directory is empty.
     *
     * @param directory - directory to check
     * @return false if directory exists and empty, true otherwise.
     * @throws IllegalArgumentException if specified directory represent a file
     * @throws IOException if some problem encountered during reading directory content
     */
    public static boolean isEmptyDirectory( File directory ) throws IOException
    {
        if ( directory.exists() )
        {
            if ( !directory.isDirectory() )
            {
                throw new IllegalArgumentException( "Expected directory, but was file: " + directory );
            }
            else
            {
                try ( DirectoryStream<Path> directoryStream = Files.newDirectoryStream( directory.toPath() ) )
                {
                    return !directoryStream.iterator().hasNext();
                }
            }
        }
        return true;
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
     * Calculates the size of a given directory or file given the provided abstract filesystem.
     *
     * @param fs the filesystem abstraction to use
     * @param file to the file or directory.
     * @return the size, in bytes, of the file or the total size of the content in the directory, including
     * subdirectories.
     */
    public static long size( FileSystemAbstraction fs, File file )
    {
        if ( fs.isDirectory( file ) )
        {
            long size = 0L;
            File[] files = fs.listFiles( file );
            if ( files == null )
            {
                return 0L;
            }
            for ( File child : files )
            {
                size += size( fs, child );
            }
            return size;
        }
        else
        {
            return fs.getFileSize( file );
        }
    }
}
