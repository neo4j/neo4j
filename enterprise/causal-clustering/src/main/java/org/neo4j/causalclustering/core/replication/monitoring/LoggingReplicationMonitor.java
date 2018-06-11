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
package org.neo4j.causalclustering.core.replication.monitoring;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.logging.Log;

import static java.lang.String.format;

public class LoggingReplicationMonitor implements ReplicationMonitor
{
    private final Log log;
    private int attempts;
    private ReplicatedContent current;

    public LoggingReplicationMonitor( Log log )
    {
        this.log = log;
    }

    @Override
    public void startReplication( ReplicatedContent command )
    {
        clear( command );
    }

    @Override
    public void replicationAttempt()
    {
        if ( attempts > 0 )
        {
            log.warn( "Failed to replicate content. Attempt %d, Content: %s", attempts, current );
        }
        attempts++;
    }

    @Override
    public void successfulReplication()
    {
        clear( null );
    }

    @Override
    public void failedReplication( Throwable t )
    {
        clear( null );
        log.error( "Failed to replicate content", t );
    }

    private void clear( ReplicatedContent o )
    {
        current = o;
        attempts = 0;
    }
}
