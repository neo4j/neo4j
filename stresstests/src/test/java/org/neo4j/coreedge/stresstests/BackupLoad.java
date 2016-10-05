/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.stresstests;

import java.io.File;
import java.net.ConnectException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;

import org.neo4j.backup.OnlineBackup;
import org.neo4j.com.ComException;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.helpers.SocketAddress;

class BackupLoad extends RepeatUntilOnSelectedMemberCallable
{
    private final File baseDirectory;
    private final BiFunction<Boolean,Integer,SocketAddress> backupAddress;

    BackupLoad( BooleanSupplier keepGoing, Runnable onFailure, Cluster cluster, File baseDirectory,
            BiFunction<Boolean,Integer,SocketAddress> backupAddress )
    {
        super( keepGoing, onFailure, cluster, cluster.edgeMembers().isEmpty() );
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
            backup = OnlineBackup.from( address.getHostname(), address.getPort() ).backup( backupDirectory );
        }
        catch ( RuntimeException e )
        {
            if ( isConnectionError( e ) )
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

    private boolean isConnectionError( RuntimeException e )
    {
        return e.getCause() instanceof ConnectException || isChannelClosedException( e ) ||
                isChannelClosedException( e.getCause() );
    }

    private boolean isChannelClosedException( Throwable e )
    {
        return e instanceof ComException && "Channel has been closed".equals( e.getMessage() );
    }
}
