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
package org.neo4j.causalclustering.backup.backup_stores;

import java.io.File;
import java.util.UUID;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;

import static org.neo4j.causalclustering.helpers.BackupUtil.createBackupFromCore;

public abstract class AbstractStoreGenerator implements BackupStore
{
    private File backup;

    abstract CoreGraphDatabase createData( Cluster cluster ) throws Exception;

    abstract void modify( File backup );

    private void generate( File backupDir, Cluster backupCluster ) throws Exception
    {
        CoreGraphDatabase db = createData( backupCluster );
        this.backup = createBackupFromCore( db, backupName(), backupDir );
        modify( backup );
    }

    @Override
    public File get( File backupDir, Cluster backupCluster ) throws Exception
    {
        if ( backup == null )
        {
            generate( backupDir, backupCluster );
        }
        return backup;
    }

    private String backupName()
    {
        return "backup-" + UUID.randomUUID().toString().substring( 5 );
    }
}
