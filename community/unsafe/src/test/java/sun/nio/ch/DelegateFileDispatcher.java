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
package sun.nio.ch;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.nio.channels.SelectableChannel;

import org.neo4j.unsafe.impl.internal.dragons.LookupBypass;

public class DelegateFileDispatcher extends AccessibleFileDisptacher
{
    private final FileDispatcher delegate;

    public DelegateFileDispatcher( Object delegate )
    {
        this.delegate = (FileDispatcher) delegate;
    }

    // Specific to all platforms other than Oracle JDK 7 on Linux, it seems
    public int force( FileDescriptor fd, boolean metaData ) throws IOException
    {
        try
        {
            return (int) forceHandle.invokeExact( delegate, fd, metaData );
        }
        catch ( Throwable throwable )
        {
            throw new LinkageError( "not linked: FileDispatcher.force(FileDescriptor, boolean)", throwable );
        }
    }

    // Specific to Oracle JDK 7 on Linux
    public int force( FileDescriptor fd, boolean metaData, boolean writable ) throws IOException
    {
        try
        {
            return (int) forceHandle.invokeExact( delegate, fd, metaData, writable );
        }
        catch ( Throwable throwable )
        {
            throw new LinkageError( "not linked: FileDispatcher.force(FileDescriptor, boolean, boolean)", throwable );
        }
    }

    private static final MethodHandle forceHandle = getForceHandle();

    private static MethodHandle getForceHandle()
    {
        LookupBypass lookup = new LookupBypass();
        MethodType arg2 = MethodType.methodType( int.class, FileDescriptor.class, boolean.class );
        MethodType arg3 = MethodType.methodType( int.class, FileDescriptor.class, boolean.class, boolean.class );
        MethodHandle handle = lookupFileDispatcherHandle( lookup, "force", arg2, false );
        if ( handle == null )
        {
            handle = lookupFileDispatcherHandle( lookup, "force", arg3, true );
        }
        return handle;
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

    // new method introduced in oracle 8u60 and 7u80
    boolean transferToDirectlyNeedsPositionLock()
    {
        if ( transferToDirectlyNeedsPositionLockHandle == null )
        {
            // whatever it should not be called...
            return false;
        }

        try
        {
            return (Boolean) transferToDirectlyNeedsPositionLockHandle.invokeExact( delegate );
        }
        catch ( Throwable throwable )
        {
            throw new LinkageError( "not linked: FileDispatcher.transferToDirectlyNeedsPositionLock()", throwable );
        }
    }

    private static final MethodHandle transferToDirectlyNeedsPositionLockHandle =
            getTransferToDirectlyNeedsPositionLockHandle();

    public static MethodHandle getTransferToDirectlyNeedsPositionLockHandle()
    {
        LookupBypass lookup = new LookupBypass();
        MethodType methodType = MethodType.methodType( boolean.class );
        return lookupFileDispatcherHandle( lookup, "transferToDirectlyNeedsPositionLock", methodType, false );
    }

    // new method introduced in oracle 8u60 and 7u80
    boolean canTransferToDirectly( SelectableChannel selectableChannel )
    {
        if ( canTransferToDirectlyHandle == null )
        {
            // whatever it should not be called...
            return false;
        }

        try
        {
            return (Boolean) canTransferToDirectlyHandle.invokeExact( delegate, selectableChannel );
        }
        catch ( Throwable throwable )
        {
            throw new LinkageError( "not linked: FileDispatcher.canTransferToDirectly(SelectableChannel)", throwable );
        }
    }

    private static final MethodHandle canTransferToDirectlyHandle = getCanTransferToDirectlyHandle();

    public static MethodHandle getCanTransferToDirectlyHandle()
    {
        LookupBypass lookup = new LookupBypass();
        MethodType methodType = MethodType.methodType( boolean.class, SelectableChannel.class );
        return lookupFileDispatcherHandle( lookup, "canTransferToDirectly", methodType, false );
    }

    // utility method
    private static MethodHandle lookupFileDispatcherHandle(
            LookupBypass lookup, String name, MethodType type, boolean throwOnError )
    {
        try
        {
            Class<FileDispatcher> cls = FileDispatcher.class;
            Method method = cls.getDeclaredMethod( name, type.parameterArray() );
            method.setAccessible( true );
            return lookup.unreflect( method ); // We have to unreflect because we need to setAccessible( true )
        }
        catch ( Exception e )
        {
            if ( throwOnError )
            {
                throw new LinkageError( "No such FileDispatcher method: " + name + " of type " + type );
            }
            return null;
        }
    }
}
