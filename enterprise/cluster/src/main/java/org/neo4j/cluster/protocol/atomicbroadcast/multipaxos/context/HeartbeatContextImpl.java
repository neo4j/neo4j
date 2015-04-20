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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerContext;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.helpers.collection.Iterables.toList;

class HeartbeatContextImpl
    extends AbstractContextImpl
    implements HeartbeatContext
{
    // HeartbeatContext
    private Set<InstanceId> failed = new HashSet<InstanceId>();

    private Map<InstanceId, Set<InstanceId>> nodeSuspicions = new HashMap<InstanceId, Set<InstanceId>>();

    private Iterable<HeartbeatListener> heartBeatListeners = Listeners.newListeners();

    private final Executor executor;
    private ClusterContext clusterContext;
    private LearnerContext learnerContext;

    HeartbeatContextImpl( InstanceId me, CommonContextState commonState, Logging logging,
                          Timeouts timeouts, Executor executor )
    {
        super( me, commonState, logging, timeouts );
        this.executor = executor;
    }

    private HeartbeatContextImpl( InstanceId me, CommonContextState commonState, Logging logging, Timeouts timeouts,
                          Set<InstanceId> failed, Map<InstanceId, Set<InstanceId>> nodeSuspicions,
                          Iterable<HeartbeatListener> heartBeatListeners, Executor executor)
    {
        super( me, commonState, logging, timeouts );
        this.failed = failed;
        this.nodeSuspicions = nodeSuspicions;
        this.heartBeatListeners = heartBeatListeners;
        this.executor = executor;
    }

    public void setCircularDependencies( ClusterContext clusterContext, LearnerContext learnerContext )
    {
        this.clusterContext = clusterContext;
        this.learnerContext = learnerContext;
    }

    @Override
    public void started()
    {
        failed.clear();
    }

    /**
     * @return True iff the node was suspected
     */
    @Override
    public boolean alive( final InstanceId node )
    {
        Set<InstanceId> serverSuspicions = suspicionsFor( getMyId() );
        boolean suspected = serverSuspicions.remove( node );

        if ( !isFailed( node ) && failed.remove( node ) )
        {
            getLogger( HeartbeatContext.class ).info( "Notifying listeners that instance " + node + " is alive" );
            Listeners.notifyListeners( heartBeatListeners, executor, new Listeners.Notification<HeartbeatListener>()
            {
                @Override
                public void notify( HeartbeatListener listener )
                {
                    listener.alive( node );
                }
            } );
        }

        return suspected;
    }

    @Override
    public void suspect( final InstanceId node )
    {
        Set<InstanceId> serverSuspicions = suspicionsFor( getMyId() );

        if ( !serverSuspicions.contains( node ) )
        {
            serverSuspicions.add( node );

            getLogger( HeartbeatContext.class ).info( getMyId() + "(me) is now suspecting " + node );
        }

        if ( isFailed( node ) && !failed.contains( node ) )
        {
            getLogger( HeartbeatContext.class ).info( "Notifying listeners that instance " + node + " is failed" );
            failed.add( node );
            Listeners.notifyListeners( heartBeatListeners, executor, new Listeners.Notification<HeartbeatListener>()
            {
                @Override
                public void notify( HeartbeatListener listener )
                {
                    listener.failed( node );
                }
            } );
        }
    }

    @Override
    public void suspicions( InstanceId from, Set<InstanceId> suspicions )
    {
        Set<InstanceId> serverSuspicions = suspicionsFor( from );

        // Check removals
        Iterator<InstanceId> suspicionsIterator = serverSuspicions.iterator();
        while ( suspicionsIterator.hasNext() )
        {
            InstanceId currentSuspicion = suspicionsIterator.next();
            if ( !suspicions.contains( currentSuspicion ) )
            {
                getLogger( HeartbeatContext.class ).info( from + " is no longer suspecting " + currentSuspicion );
                suspicionsIterator.remove();
            }
        }

        // Check additions
        for ( InstanceId suspicion : suspicions )
        {
            if ( !serverSuspicions.contains( suspicion ) )
            {
                getLogger( HeartbeatContext.class ).info( from + " is now suspecting " + suspicion );
                serverSuspicions.add( suspicion );
            }
        }

        // Check if anyone is considered failed
        for ( final InstanceId node : suspicions )
        {
            if ( isFailed( node ) && !failed.contains( node ) )
            {
                failed.add( node );
                Listeners.notifyListeners( heartBeatListeners, executor, new Listeners.Notification<HeartbeatListener>()
                {
                    @Override
                    public void notify( HeartbeatListener listener )
                    {
                        listener.failed( node );
                    }
                } );
            }
        }
    }

    @Override
    public Set<InstanceId> getFailed()
    {
        return failed;
    }

    @Override
    public Iterable<InstanceId> getAlive()
    {
        return Iterables.filter( new Predicate<InstanceId>()
        {
            @Override
            public boolean accept( InstanceId item )
            {
                return !isFailed( item );
            }
        }, commonState.configuration().getMemberIds() );
    }

    @Override
    public void addHeartbeatListener( HeartbeatListener listener )
    {
        heartBeatListeners = Listeners.addListener( listener, heartBeatListeners );
    }

    @Override
    public void removeHeartbeatListener( HeartbeatListener listener )
    {
        heartBeatListeners = Listeners.removeListener( listener, heartBeatListeners );
    }

    @Override
    public void serverLeftCluster( InstanceId node )
    {
        failed.remove( node );
        for ( Set<InstanceId> uris : nodeSuspicions.values() )
        {
            uris.remove( node );
        }
    }

    @Override
    public boolean isFailed( InstanceId node )
    {
        List<InstanceId> suspicions = getSuspicionsOf( node );

        /*
         * This looks weird but trust me, there is a reason for it.
         * See below in the test, where we subtract the failed size() from the total cluster size? If the instance
         * under question is already in the failed set then that's it, as expected. But if it is not in the failed set
         * then we must not take it's opinion under consideration (which we implicitly don't for every member of the
         * failed set). That's what the adjust represents - the node's opinion on whether it is alive or not. Run a
         * 3 cluster simulation in your head with 2 instances failed and one coming back online and you'll see why.
         */
        int adjust = failed.contains( node ) ? 0 : 1;

        // If more than half suspect this node, fail it
        return suspicions.size() >
                (commonState.configuration().getMembers().size() - failed.size() - adjust) / 2;
    }

    @Override
    public List<InstanceId> getSuspicionsOf( InstanceId server )
    {
        List<InstanceId> suspicions = new ArrayList<InstanceId>();
        for ( InstanceId member : commonState.configuration().getMemberIds() )
        {
            Set<InstanceId> memberSuspicions = nodeSuspicions.get( member );
            if ( memberSuspicions != null && !failed.contains( member )
                    && memberSuspicions.contains( server ) )
            {
                suspicions.add( member );
            }
        }

        return suspicions;
    }

    @Override
    public Set<InstanceId> getSuspicionsFor( InstanceId uri )
    {
        Set<org.neo4j.cluster.InstanceId> suspicions = suspicionsFor( uri );
        return new HashSet<org.neo4j.cluster.InstanceId>( suspicions );
    }

    private Set<InstanceId> suspicionsFor( InstanceId uri )
    {
        Set<InstanceId> serverSuspicions = nodeSuspicions.get( uri );
        if ( serverSuspicions == null )
        {
            serverSuspicions = new HashSet<InstanceId>();
            nodeSuspicions.put( uri, serverSuspicions );
        }
        return serverSuspicions;
    }

    @Override
    public Iterable<InstanceId> getOtherInstances()
    {
        return clusterContext.getOtherInstances();
    }

    @Override
    public long getLastKnownLearnedInstanceInCluster()
    {
        return learnerContext.getLastKnownLearnedInstanceInCluster();
    }

    @Override
    public long getLastLearnedInstanceId()
    {
        return learnerContext.getLastLearnedInstanceId();
    }

    public HeartbeatContextImpl snapshot( CommonContextState commonStateSnapshot, Logging logging, Timeouts timeouts,
                                          Executor executor )
    {
        return new HeartbeatContextImpl( me, commonStateSnapshot, logging, timeouts, new HashSet<>(failed),
                new HashMap<>(nodeSuspicions), new ArrayList<>(toList(heartBeatListeners)), executor );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        HeartbeatContextImpl that = (HeartbeatContextImpl) o;

        if ( failed != null ? !failed.equals( that.failed ) : that.failed != null )
        {
            return false;
        }
        if ( nodeSuspicions != null ? !nodeSuspicions.equals( that.nodeSuspicions ) : that.nodeSuspicions != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = failed != null ? failed.hashCode() : 0;
        result = 31 * result + (nodeSuspicions != null ? nodeSuspicions.hashCode() : 0);
        return result;
    }
}
