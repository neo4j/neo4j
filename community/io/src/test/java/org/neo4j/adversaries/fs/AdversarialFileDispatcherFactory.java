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
package org.neo4j.adversaries.fs;

import sun.nio.ch.DelegateFileDispatcher;

import java.io.FileDescriptor;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.adversaries.Adversary;

@SuppressWarnings( "unchecked" )
public class AdversarialFileDispatcherFactory
{
    public static Object makeFileDispatcherAdversarial(
            final Object delegateFileDispatcher,
            final Adversary adversary ) throws Exception
    {
        return new DelegateFileDispatcher( delegateFileDispatcher )
        {
            private int mischievousLength( int len )
            {
                if ( adversary.injectFailureOrMischief( IOException.class ) )
                {
                    // We cannot pass zero lengths, because they are checked for and
                    // circumvented in higher-level NIO code.
                    len = len == 1? 1 : ThreadLocalRandom.current().nextInt( 1, len );
                }
                return len;
            }

            @Override
            public long readv( FileDescriptor fd, long address, int len ) throws IOException
            {
                return super.readv( fd, address, mischievousLength( len ) );
            }

            @Override
            public int read( FileDescriptor fd, long address, int len ) throws IOException
            {
                return super.read( fd, address, mischievousLength( len ) );
            }

            @Override
            public int pwrite( FileDescriptor fd, long address, int len, long position ) throws IOException
            {
                return super.pwrite( fd, address, mischievousLength( len ), position );
            }

            @Override
            public int truncate( FileDescriptor fd, long size ) throws IOException
            {
                adversary.injectFailure( IOException.class );
                return super.truncate( fd, size );
            }

            @Override
            public int pread( FileDescriptor fd, long address, int len, long position ) throws IOException
            {
                return super.pread( fd, address, mischievousLength( len ), position );
            }

            @Override
            public long writev( FileDescriptor fd, long address, int len ) throws IOException
            {
                return super.writev( fd, address, mischievousLength( len ) );
            }

            @Override
            public int write( FileDescriptor fd, long address, int len ) throws IOException
            {
                return super.write( fd, address, mischievousLength( len ) );
            }
        };
    }
}
