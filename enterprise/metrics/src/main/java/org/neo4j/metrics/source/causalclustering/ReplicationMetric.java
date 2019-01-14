/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.metrics.source.causalclustering;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.causalclustering.core.replication.monitoring.ReplicationMonitor;

public class ReplicationMetric implements ReplicationMonitor
{
    private final AtomicLong  newReplication = new AtomicLong(  );
    private final AtomicLong attempts = new AtomicLong(  );
    private final AtomicLong success = new AtomicLong(  );
    private final AtomicLong fail = new AtomicLong(  );

    @Override
    public void startReplication()
    {
        newReplication.getAndIncrement();
    }

    @Override
    public void replicationAttempt()
    {
        attempts.getAndIncrement();
    }

    @Override
    public void successfulReplication()
    {
        success.getAndIncrement();
    }

    @Override
    public void failedReplication( Throwable t )
    {
        fail.getAndIncrement();
    }

    public long newReplicationCount()
    {
        return newReplication.get();
    }

    public long attemptCount()
    {
        return attempts.get();
    }

    public long successCount()
    {
        return success.get();
    }

    public long failCount()
    {
        return fail.get();
    }
}
