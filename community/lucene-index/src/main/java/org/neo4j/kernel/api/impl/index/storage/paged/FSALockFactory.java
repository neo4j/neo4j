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
package org.neo4j.kernel.api.impl.index.storage.paged;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;

/**
 * Clone of {@link NativeFSLockFactory} for {@link FileSystemAbstraction}.
 */
public class FSALockFactory extends LockFactory
{
    // Lucene implements concurrency across multiple Directory instances
    // by using file locks; this way, concurrency is permitted across processes.
    // However, Neo4j does not do cross-process concurrent access to store
    // files. Meaning, we are paying for all these file locks for no reason..
    //
    // Lucene comes with an alternate locking implementation,
    // SingleInstanceLockFactory, for use cases where it can be guaranteed a
    // single Directory instance will be used at a time; we can give this
    // guarantee.
    //
    // Hence: If we refactored our three (explicit, schema, full-text) usages
    // of Lucene to use a single global Directory instance, we could get a
    // performance boost by removing the need for file-system locks for Lucene.

    private static final Set<String> LOCKS_HELD = Collections.synchronizedSet( new HashSet<String>() );

    private final FileSystemAbstraction fs;

    public FSALockFactory( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    @Override
    public Lock obtainLock( Directory dir, String lockName ) throws IOException
    {
        if ( !(dir instanceof PagedDirectory) )
        {
            throw new IOException( "FSALockFactory can only be used in " + "combination with PagedDirectory." );
        }
        Path lockDir = ((PagedDirectory) dir).getPath();

        // Ensure that lockDir exists and is a directory.
        fs.mkdirs( lockDir.toFile() );

        Path lockFile = lockDir.resolve( lockName );

        try
        {
            fs.create( lockFile.toFile() ).close();
        }
        catch ( IOException ignore )
        {
            // we must create the file to have a truly canonical path.
            // if it's already created, we don't care.
            // if it cant be created, it will fail below.
        }

        final Path absPath = lockFile.toAbsolutePath();

        // used as a best-effort check, to see if the underlying file has changed
        final long creationTime = fs.lastModifiedTime( absPath.toFile() );

        if ( LOCKS_HELD.add( absPath.toString() ) )
        {
            StoreChannel channel = null;
            FileLock lock = null;
            try
            {
                channel = fs.open( absPath.toFile(), OpenMode.READ_WRITE );
                lock = channel.tryLock();
                if ( lock != null )
                {
                    return new FSALock( fs, lock, absPath, creationTime );
                }
                else
                {
                    throw new LockObtainFailedException( "Lock held by another program: " + absPath );
                }
            }
            finally
            {
                if ( lock == null )
                {
                    // not successful - clear up and move out
                    IOUtils.closeWhileHandlingException( channel );
                    clearLockHeld( absPath );  // clear LOCKS_HELD last
                }
            }
        }
        else
        {
            throw new LockObtainFailedException( "Lock held by this virtual machine: " + absPath );
        }
    }

    private static void clearLockHeld( Path path )
    {
        boolean remove = LOCKS_HELD.remove( path.toString() );
        if ( !remove )
        {
            throw new AlreadyClosedException( "Lock path was cleared but never marked as held: " + path );
        }
    }

    static final class FSALock extends Lock
    {
        final FileSystemAbstraction fs;
        final FileLock lock;
        final Path path;
        final long creationTime;
        volatile boolean closed;

        FSALock( FileSystemAbstraction fs, FileLock lock, Path path, long creationTime )
        {
            this.fs = fs;
            this.lock = lock;
            this.path = path;
            this.creationTime = creationTime;
        }

        @Override
        public void ensureValid() throws IOException
        {
            if ( closed )
            {
                throw new AlreadyClosedException( "Lock instance already released: " + this );
            }
            // check we are still in the locks map
            // (some debugger or something crazy didn't remove us)
            if ( !LOCKS_HELD.contains( path.toString() ) )
            {
                throw new AlreadyClosedException( "Lock path unexpectedly cleared from map: " + this );
            }
            // check our lock wasn't invalidated.
            if ( !lock.isValid() )
            {
                throw new AlreadyClosedException( "FileLock invalidated by an external force: " + this );
            }
            // try to validate the underlying file descriptor.
            // this will throw IOException if something is wrong.
            long size = lock.channel().size();
            if ( size != 0 )
            {
                throw new AlreadyClosedException( "Unexpected lock file size: " + size + ", (lock=" + this + ")" );
            }
            // try to validate the backing file name, that it still exists,
            // and has the same creation time as when we obtained the lock.
            // if it differs, someone deleted our lock file
            long ctime = fs.lastModifiedTime( path.toFile() );
            if ( creationTime != ctime )
            {
                throw new AlreadyClosedException(
                        "Underlying file changed by an external force at " + creationTime + ", (lock=" + this + ")" );
            }
        }

        @Override
        public synchronized void close() throws IOException
        {
            if ( closed )
            {
                return;
            }
            try ( FileChannel channel = lock.channel(); FileLock ignored = this.lock )
            {
                assert channel != null;
            }
            finally
            {
                closed = true;
                clearLockHeld( path );
            }
        }

        @Override
        public String toString()
        {
            return "FSALock(path=" + path + ",impl=" + lock + ",ctime=" + creationTime + ")";
        }
    }
}
