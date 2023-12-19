/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.ha.correctness;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.helpers.collection.PrefetchingIterator;

import static org.neo4j.helpers.collection.Iterables.filter;

/**
 * A picture of the state of the cluster, including all messages waiting to get delivered.
 * Important note: ClusterStates are equal on the states of the instances, not on the pending messages. Two states
 * with the same cluster states but different pending messages will be considered equal.
 */
class ClusterState
{
    private static final Predicate<ClusterInstance> HAS_TIMEOUTS = ClusterInstance::hasPendingTimeouts;
    private final Set<ClusterAction> pendingActions;
    private final List<ClusterInstance> instances = new ArrayList<>();

    ClusterState( List<ClusterInstance> instances, Set<ClusterAction> pendingActions )
    {
        this.pendingActions = pendingActions instanceof LinkedHashSet ? pendingActions
                : new LinkedHashSet<>( pendingActions );
        this.instances.addAll( instances );
    }

    public void addPendingActions( ClusterAction ... actions )
    {
        pendingActions.addAll( Arrays.asList( actions ) );
    }

    /** All possible new cluster states that can be generated from this one. */
    public Iterator<Pair<ClusterAction,ClusterState>> transitions()
    {
        final Iterator<ClusterAction> actions = pendingActions.iterator();
        final Iterator<ClusterInstance> instancesWithTimeouts = filter( HAS_TIMEOUTS, instances ).iterator();
        return new PrefetchingIterator<Pair<ClusterAction,ClusterState>>()
        {
            @Override
            protected Pair<ClusterAction,ClusterState> fetchNextOrNull()
            {
                try
                {
                    if ( actions.hasNext() )
                    {
                        ClusterAction action = actions.next();
                        return Pair.of( action, performAction( action ) );
                    }
                    else if ( instancesWithTimeouts.hasNext() )
                    {
                        ClusterInstance instance = instancesWithTimeouts.next();
                        return performNextTimeoutFrom( instance );
                    }
                    else
                    {
                        return null;
                    }
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }

    /** Managing timeouts is trickier than putting all of them in a long list, like regular message delivery.
     *  Timeouts are ordered and can be cancelled, so they need special treatment. Hence a separate method for
     *  managing timeouts triggering. */
    private Pair<ClusterAction, ClusterState> performNextTimeoutFrom( ClusterInstance instance ) throws
            Exception
    {
        ClusterState newState = snapshot();
        ClusterAction clusterAction = newState.instance( instance.uri().toASCIIString() ).popTimeout();
        clusterAction.perform( newState );

        return Pair.of(clusterAction, newState);
    }

    /** Clone the state and perform the action with the provided index. Returns the new state and the action. */
    ClusterState performAction( ClusterAction action ) throws Exception
    {
        ClusterState newState = snapshot();

        // Remove the action from the list of things that can happen in the snapshot
        newState.pendingActions.remove( action );

        // Perform the action on the cloned state
        Iterable<ClusterAction> newActions = action.perform( newState );

        // Include any outcome actions into the new state snapshot
        newState.pendingActions.addAll( Iterables.asCollection(newActions) );

        return newState;
    }

    public ClusterState snapshot()
    {
        Set<ClusterAction> newPendingActions = new LinkedHashSet<>( pendingActions );

        // Clone the current state & perform the action on it to change it
        List<ClusterInstance> cloneInstances = new ArrayList<>();
        for ( ClusterInstance clusterInstance : instances )
        {
            cloneInstances.add( clusterInstance.newCopy() );
        }

        return new ClusterState( cloneInstances, newPendingActions );
    }

    public ClusterInstance instance( String to ) throws URISyntaxException
    {
        URI uri = new URI( to );
        for ( ClusterInstance clusterInstance : instances )
        {
            URI instanceUri = clusterInstance.uri();
            if ( instanceUri.getHost().equals( uri.getHost() ) && instanceUri.getPort() == uri.getPort() )
            {
                return clusterInstance;
            }
        }

        throw new IllegalArgumentException( "No instance in cluster at address: " + to );
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

        ClusterState that = (ClusterState) o;

        return instances.equals( that.instances ) && pendingActions.equals( that.pendingActions );
    }

    @Override
    public int hashCode()
    {
        int result = instances.hashCode();
        result = 31 * result + pendingActions.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "Cluster[" + Iterables.toString( instances, ", " ) + "]";
    }

    public boolean isDeadEnd()
    {
        if ( pendingActions.size() > 0 )
        {
            return false;
        }

        for ( ClusterInstance instance : instances )
        {
            if ( instance.hasPendingTimeouts() )
            {
                return false;
            }
        }

        return true;
    }
}
