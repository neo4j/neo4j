/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.locks;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.coreedge.raft.LeadershipChange;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.UUID.randomUUID;

/**
 * The core service manager manages the local core service registry. The core service registry
 * contains the local view of service assignment within the core group, and allows for core
 * services to be looked up.
 *
 * The core service manager listens to and spawns core service assignments over the replicator.
 *
 * Currently only one policy for assignment is in effect:
 *      when this node becomes leader => assign it as the lock manager
 */
public class CoreServiceManager implements Listener<LeadershipChange<CoreMember>>, Replicator.ReplicatedContentListener
{
    final private Replicator replicator;
    final private CoreServiceRegistry serviceRegistry;
    private final CoreMember myself;
    final private ExecutorService executor = Executors.newCachedThreadPool();
    private final Log log;

    public CoreServiceManager( Replicator replicator, CoreServiceRegistry serviceRegistry,
                               CoreMember myself, LogProvider logProvider )
    {
        this.replicator = replicator;
        this.serviceRegistry = serviceRegistry;
        this.myself = myself;
        this.log = logProvider.getLog( getClass() );

        replicator.subscribe( this );
    }

    @Override
    public void onReplicated( ReplicatedContent content )
    {
        if ( content instanceof CoreServiceAssignment )
        {
            CoreServiceAssignment serviceAssignment = (CoreServiceAssignment) content;
            log.info( "Committed assignment: " + serviceAssignment );
            serviceRegistry.registerProvider( serviceAssignment.serviceType(), serviceAssignment );
        }
    }

    @Override
    public void receive( final LeadershipChange<CoreMember> leadershipChange )
    {
        final CoreMember leader = leadershipChange.leader();

        if ( leader == myself )
        {
            executor.execute( new Runnable()
            {
                @Override
                public void run()
                {
                    assignLockManager( leader );
                }
            } );
        }
    }

    private void assignLockManager( CoreMember lockManager )
    {
        CoreServiceAssignment assignment = new CoreServiceAssignment( CoreServiceRegistry.ServiceType.LOCK_MANAGER, lockManager, randomUUID() );

        serviceRegistry.registerProvider( CoreServiceRegistry.ServiceType.LOCK_MANAGER, assignment );

        try
        {
            replicator.replicate( assignment );
        }
        catch ( Replicator.ReplicationFailedException e )
        {
            e.printStackTrace();
        }
    }
}
