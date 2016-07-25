/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.impl.lucene.legacy;

import java.io.IOException;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;

public class MultipleBackupDeletionPolicy extends SnapshotDeletionPolicy
{
    static final String SNAPSHOT_ID = "backup";

    private IndexCommit snapshot;
    private int snapshotUsers;

    public MultipleBackupDeletionPolicy()
    {
        super( new KeepOnlyLastCommitDeletionPolicy() );
    }

    @Override
    public synchronized IndexCommit snapshot() throws IOException
    {
        if ( snapshotUsers == 0 )
        {
            snapshot = super.snapshot();
        }
        // Incremented after the call to super.snapshot() so that it wont get incremented
        // if an exception (IllegalStateException if empty index) is thrown
        snapshotUsers++;
        return snapshot;
    }

    @Override
    public synchronized void release( IndexCommit commit ) throws IOException
    {
        if ( (--snapshotUsers) > 0 )
        {
            return;
        }
        super.release( commit );
        snapshot = null;
        if ( snapshotUsers < 0 )
        {
            snapshotUsers = 0;
            throw new IllegalStateException( "Cannot release snapshot, no snapshot held" );
        }
    }
}
