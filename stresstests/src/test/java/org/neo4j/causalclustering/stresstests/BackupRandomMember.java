/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.function.Predicate;

import org.neo4j.causalclustering.BackupUtil;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.helper.IsChannelClosedException;
import org.neo4j.helper.IsConnectionException;
import org.neo4j.helper.IsConnectionResetByPeer;
import org.neo4j.helper.IsStoreClosed;

// TODO: Add validation for backup count? Must have at least 1 and X % successful or something? Depending on scenario?
class BackupRandomMember extends RepeatOnRandomMember
{
    private final Predicate<Throwable> isTransientError = new IsConnectionException()
            .or( new IsConnectionResetByPeer() )
            .or( new IsChannelClosedException() )
            .or( new IsStoreClosed() );

    private final File baseBackupDir;
    private long backupNumber;

    BackupRandomMember( Control control, Resources resources )
    {
        super( control, resources );
        this.baseBackupDir = resources.backupDir();
    }

    @Override
    protected void doWorkOnMember( ClusterMember member ) throws Exception
    {
        try
        {
            String backupName = "backup-" + backupNumber++;
            BackupUtil.createBackupInProcess( member, baseBackupDir, backupName );
        }
        catch ( RuntimeException e )
        {
            if ( !isTransientError.test( e ) )
            {
                throw e;
            }
        }
    }
}
