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
package org.neo4j.metrics.source.causalclustering;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.replication.monitoring.ReplicationMonitor;

public class ReplicationMetric implements ReplicationMonitor
{
    private long newReplication;
    private long attempts;
    private long success;
    private long fail;

    @Override
    public void startReplication( ReplicatedContent command )
    {
        newReplication++;
    }

    @Override
    public void replicationAttempt()
    {
        attempts++;
    }

    @Override
    public void successfulReplication()
    {
        success++;
    }

    @Override
    public void failedReplication( Throwable t )
    {
        fail++;
    }

    public long newReplicationCount()
    {
        return newReplication;
    }

    public long attemptCount()
    {
        return attempts;
    }

    public long successCount()
    {
        return success;
    }

    public long failCount()
    {
        return fail;
    }
}
