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
package org.neo4j.causalclustering.stresstests;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import org.neo4j.helper.IsChannelClosedException;
import org.neo4j.helper.IsConnectionException;
import org.neo4j.helper.IsConnectionRestByPeer;
import org.neo4j.helper.IsStoreClosed;
import org.neo4j.backup.OnlineBackup;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.helpers.SocketAddress;

class BackupLoad extends RepeatUntilOnSelectedMemberCallable
{
    private static final OutputStream NULL_OUTPUT_STREAM =  new OutputStream()
    {
        @Override
        public void write( int b ) throws IOException
        {
            // it's *null* output stream
        }
    };

    private final Predicate<Throwable> isTransientError =
            new IsConnectionException().or( new IsConnectionRestByPeer() ).or( new IsChannelClosedException() )
                    .or( new IsStoreClosed() );

    private final File baseDirectory;
    private final BiFunction<Boolean,Integer,SocketAddress> backupAddress;

    BackupLoad( BooleanSupplier keepGoing, Runnable onFailure, Cluster cluster, int numberOfCores, int numberOfEdges,
            File baseDirectory, BiFunction<Boolean,Integer,SocketAddress> backupAddress )
    {
        super( keepGoing, onFailure, cluster, numberOfCores, numberOfEdges );
        this.baseDirectory = baseDirectory;
        this.backupAddress = backupAddress;
    }

    @Override
    protected void doWorkOnMember( boolean isCore, int id )
    {
        SocketAddress address = backupAddress.apply( isCore, id );
        File backupDirectory = new File( baseDirectory, Integer.toString( address.getPort() ) );

        OnlineBackup backup;
        try
        {
            backup = OnlineBackup.from( address.getHostname(), address.getPort() ).withOutput( NULL_OUTPUT_STREAM )
                    .backup( backupDirectory );
        }
        catch ( RuntimeException e )
        {
            if ( isTransientError.test( e ) )
            {
                // if we could not connect, wait a bit and try again...
                LockSupport.parkNanos( 10_000_000 );
                return;
            }
            throw e;
        }

        if ( !backup.isConsistent() )
        {
            throw new RuntimeException( "Not consistent backup from " + address );
        }
    }
}
