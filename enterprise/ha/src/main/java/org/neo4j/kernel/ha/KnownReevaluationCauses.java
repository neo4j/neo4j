/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import static org.neo4j.kernel.ha.UnknownReevaluationCause.formLogLine;

import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Lists the different causes of re-evaluation/master re-election for extra
 * information in logging.
 */
public enum KnownReevaluationCauses implements ReevaluationCause
{
    INITIAL_STARTUP(),
    ZK_SESSION_EXPIRED( "ZooKeeper session expired so needs to reconnect" ),
    ZK_INITIAL_SYNC_CONNECTED( "ZooKeeper event: initial SyncConnected" ),
    ZK_MACHINE_LEFT( "ZooKeeper event: machine left the cluster" ),
    ZK_ELECTED_MASTER( "ZooKeeper event: I have been elected master" ),
    ZK_MASTER_AVAILABLE( "ZooKeeper event: Newly elected master telling me to be slave" ),
    KERNEL_PANIC( "Tx manager not OK" ),
    COPY_STORE_FROM_MASTER( "Preparing for copying store from master" ),
    STARTING_AS_MASTER( "Starting as master (this is the initial startup)" ),
    STARTING_AS_SLAVE( "Starting as slave (this is the initial startup)" ),
    SWITCHING_FROM_MASTER_TO_SLAVE(),
    SWITCHING_FROM_SLAVE_TO_MASTER(),
    SHUTDOWN(),
    BRANCHED_DATA( "I've got branched data compared to the current master" ),
    NO_MASTER_IN_PULL_UPDATES( "I've got no master, but was told to pull updates" );
    
    private final String description;
    
    private KnownReevaluationCauses()
    {
        this( null );
    }
    
    private KnownReevaluationCauses( String description )
    {
        this.description = description;
    }
    
    @Override
    public void log( StringLogger logger, String message )
    {
        logger.logMessage( formLogLine( message, this ) , true);
    }

    @Override
    public String getDescription()
    {
        return description;
    }
}
