/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package sun.nio.ch;

import java.io.FileDescriptor;
import java.io.IOException;

public class DelegateFileDispatcher extends AccessibleFileDisptacher
{
    private final FileDispatcher delegate;

    public DelegateFileDispatcher( Object delegate )
    {
        this.delegate = (FileDispatcher) delegate;
    }

    public int force( FileDescriptor fd, boolean metaData ) throws IOException
    {
        return delegate.force( fd, metaData );
    }

    public long readv( FileDescriptor fd, long address, int len ) throws IOException
    {
        return delegate.readv( fd, address, len );
    }

    public int lock( FileDescriptor fd, boolean blocking, long pos, long size, boolean shared ) throws IOException
    {
        return delegate.lock( fd, blocking, pos, size, shared );
    }

    public FileDescriptor duplicateForMapping( FileDescriptor fd ) throws IOException
    {
        return delegate.duplicateForMapping( fd );
    }

    public int read( FileDescriptor fd, long address, int len ) throws IOException
    {
        return delegate.read( fd, address, len );
    }

    public int pwrite( FileDescriptor fd, long address, int len, long position ) throws IOException
    {
        return delegate.pwrite( fd, address, len, position );
    }

    public void preClose( FileDescriptor fd ) throws IOException
    {
        delegate.preClose( fd );
    }

    public int truncate( FileDescriptor fd, long size ) throws IOException
    {
        return delegate.truncate( fd, size );
    }

    public void release( FileDescriptor fd, long pos, long size ) throws IOException
    {
        delegate.release( fd, pos, size );
    }

    public boolean needsPositionLock()
    {
        return delegate.needsPositionLock();
    }

    public long size( FileDescriptor fd ) throws IOException
    {
        return delegate.size( fd );
    }

    public int pread( FileDescriptor fd, long address, int len, long position ) throws IOException
    {
        return delegate.pread( fd, address, len, position );
    }

    public long writev( FileDescriptor fd, long address, int len ) throws IOException
    {
        return delegate.writev( fd, address, len );
    }

    public int write( FileDescriptor fd, long address, int len ) throws IOException
    {
        return delegate.write( fd, address, len );
    }

    public void close( FileDescriptor fd ) throws IOException
    {
        delegate.close( fd );
    }
}
