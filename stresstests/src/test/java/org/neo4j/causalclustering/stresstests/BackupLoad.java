/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.stresstests;

import java.io.File;
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
            backup = OnlineBackup.from( address.getHostname(), address.getPort() ).backup( backupDirectory );
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
