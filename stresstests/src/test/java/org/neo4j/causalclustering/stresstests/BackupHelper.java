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
import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.backup.impl.OnlineBackupCommandBuilder;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;

import static org.neo4j.backup.impl.SelectedBackupProtocol.CATCHUP;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.transaction_advertised_address;
import static org.neo4j.helpers.Exceptions.findCauseOrSuppressed;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM;

class BackupHelper
{
    private static final Set<Class<? extends Throwable>> BENIGN_EXCEPTIONS = asSet(
            ConnectException.class,
            ClosedChannelException.class
    );

    AtomicLong backupNumber = new AtomicLong();
    AtomicLong successfulBackups = new AtomicLong();

    private final File baseBackupDir;
    private final Log log;

    BackupHelper( Resources resources )
    {
        this.baseBackupDir = resources.backupDir();
        this.log = resources.logProvider().getLog( getClass() );
    }

    /**
     * Performs a backup and returns the path to it. Benign failures are swallowed and an empty optional gets returned.
     *
     * @param member The member to perform the backup against.
     * @return The optional backup.
     * @throws Exception If any unexpected exceptions happen.
     */
    Optional<File> backup( ClusterMember member ) throws Exception
    {
        AdvertisedSocketAddress address = member.config().get( transaction_advertised_address );
        String backupName = "backup-" + backupNumber.getAndIncrement();

        OnlineBackupCommandBuilder backupCommand = new OnlineBackupCommandBuilder()
                .withOutput( NULL_OUTPUT_STREAM )
                .withSelectedBackupStrategy( CATCHUP )
                .withConsistencyCheck( true )
                .withHost( address.getHostname() )
                .withPort( address.getPort() );

        try
        {
            backupCommand.backup( baseBackupDir, backupName );
            log.info( String.format( "Created backup %s from %s", backupName, member ) );

            successfulBackups.incrementAndGet();

            return Optional.of( new File( baseBackupDir, backupName ) );
        }
        catch ( CommandFailed e )
        {
            Optional<Throwable> benignException = findCauseOrSuppressed( e, t -> BENIGN_EXCEPTIONS.contains( t.getClass() ) );
            if ( benignException.isPresent() )
            {
                log.info( "Benign failure: " + benignException.get().getMessage() );
            }
            else
            {
                throw e;
            }
        }
        return Optional.empty();
    }
}
