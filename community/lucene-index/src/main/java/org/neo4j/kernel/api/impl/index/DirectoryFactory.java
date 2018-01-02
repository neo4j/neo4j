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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.neo4j.io.fs.FileSystemAbstraction;

import static java.lang.Math.min;

public interface DirectoryFactory extends FileSystemAbstraction.ThirdPartyFileSystem
{
    Directory open( File dir ) throws IOException;

    /**
     * Called when the directory factory is disposed of, really only here to allow
     * the ram directory thing to close open directories.
     */
    void close();

    final DirectoryFactory PERSISTENT = new DirectoryFactory()
    {
        @SuppressWarnings("ResultOfMethodCallIgnored")
        @Override
        public Directory open( File dir ) throws IOException
        {
            dir.mkdirs();
            return FSDirectory.open( dir );
        }

        @Override
        public void close()
        {
            // No resources to release. This method only exists as a hook for test implementations.
        }

        @Override
        public void dumpToZip( ZipOutputStream zip, byte[] scratchPad )
        {
            // do nothing
        }
    };
    
    final class InMemoryDirectoryFactory implements DirectoryFactory
    {
        private final Map<File, RAMDirectory> directories = new HashMap<File, RAMDirectory>( );

        @Override
        public synchronized Directory open( File dir ) throws IOException
        {
            if(!directories.containsKey( dir ))
            {
                directories.put( dir, new RAMDirectory() );
            }
            return new UncloseableDirectory(directories.get(dir));
        }

        @Override
        public synchronized void close()
        {
            for ( RAMDirectory ramDirectory : directories.values() )
            {
                ramDirectory.close();
            }
            directories.clear();
        }

        @Override
        public void dumpToZip( ZipOutputStream zip, byte[] scratchPad ) throws IOException
        {
            for ( Map.Entry<File, RAMDirectory> entry : directories.entrySet() )
            {
                RAMDirectory ramDir = entry.getValue();
                for ( String fileName : ramDir.listAll() )
                {
                    zip.putNextEntry( new ZipEntry( new File( entry.getKey(), fileName ).getAbsolutePath() ) );
                    copy( ramDir.openInput( fileName ), zip, scratchPad );
                    zip.closeEntry();
                }
            }
        }

        private static void copy( IndexInput source, OutputStream target, byte[] buffer ) throws IOException
        {
            for ( long remaining = source.length(),read; remaining > 0;remaining -= read)
            {
                read = min( remaining, buffer.length );
                source.readBytes( buffer, 0, (int) read );
                target.write( buffer, 0, (int) read );
            }
        }
    }
    
    final class Single implements DirectoryFactory
    {
        private final Directory directory;

        public Single( Directory directory )
        {
            this.directory = directory;
        }

        @Override
        public Directory open( File dir ) throws IOException
        {
            return directory;
        }

        @Override
        public void close()
        {
        }

        @Override
        public void dumpToZip( ZipOutputStream zip, byte[] scratchPad )
        {
            throw new UnsupportedOperationException();
        }
    }

    final class UncloseableDirectory extends Directory
    {

        private final Directory delegate;

        public UncloseableDirectory(Directory delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public void close() throws IOException
        {
            // No-op
        }

        @Override
        public String[] listAll() throws IOException
        {
            return delegate.listAll();
        }

        @Override
        public boolean fileExists( String s ) throws IOException
        {
            return delegate.fileExists( s );
        }

        @Deprecated
        @Override
        public long fileModified( String s ) throws IOException
        {
            return delegate.fileModified( s );
        }

        @Override
        @Deprecated
        public void touchFile( String s ) throws IOException
        {
            delegate.touchFile( s );
        }

        @Override
        public void deleteFile( String s ) throws IOException
        {
            delegate.deleteFile( s );
        }

        @Override
        public long fileLength( String s ) throws IOException
        {
            return delegate.fileLength( s );
        }

        @Override
        public IndexOutput createOutput( String s ) throws IOException
        {
            return delegate.createOutput( s );
        }

        @Override
        @Deprecated
        public void sync( String name ) throws IOException
        {
            delegate.sync( name );
        }

        @Override
        public void sync( Collection<String> names ) throws IOException
        {
            delegate.sync( names );
        }

        @Override
        public IndexInput openInput( String s ) throws IOException
        {
            return delegate.openInput( s );
        }

        @Override
        public IndexInput openInput( String name, int bufferSize ) throws IOException
        {
            return delegate.openInput( name, bufferSize );
        }

        @Override
        public Lock makeLock( final String name )
        {
            return delegate.makeLock( name );
        }

        @Override
        public void clearLock( String name ) throws IOException
        {
            delegate.clearLock( name );
        }
        @Override
        public void setLockFactory( LockFactory lockFactory ) throws IOException
        {
            delegate.setLockFactory( lockFactory );
        }

        @Override
        public LockFactory getLockFactory()
        {
            return delegate.getLockFactory();
        }

        @Override
        public String getLockID()
        {
            return delegate.getLockID();
        }

        @Override
        public String toString()
        {
            return delegate.toString();
        }

        @Override
        public void copy( Directory to, String src, String dest ) throws IOException
        {
            delegate.copy( to, src, dest );
        }

        @Deprecated
        public static void copy( Directory src, Directory dest, boolean closeDirSrc ) throws IOException
        {
            Directory.copy( src, dest, closeDirSrc );
        }
    }
}
