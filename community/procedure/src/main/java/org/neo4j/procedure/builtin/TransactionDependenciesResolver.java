/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.procedure.builtin;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.Function;

import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.lock.ResourceType;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.neo4j.lock.LockType.EXCLUSIVE;

class TransactionDependenciesResolver
{
    private final Map<KernelTransactionHandle,Optional<QuerySnapshot>> handleSnapshotsMap;
    private final Map<KernelTransactionHandle,Set<KernelTransactionHandle>> directDependencies;

    TransactionDependenciesResolver( Map<KernelTransactionHandle,Optional<QuerySnapshot>> handleSnapshotsMap )
    {
        this.handleSnapshotsMap = handleSnapshotsMap;
        this.directDependencies = initDirectDependencies();
    }

    boolean isBlocked( KernelTransactionHandle handle )
    {
        return directDependencies.get( handle ) != null;
    }

    String describeBlockingTransactions( KernelTransactionHandle handle )
    {
        Set<KernelTransactionHandle> allBlockers = new TreeSet<>(
                Comparator.comparingLong( KernelTransactionHandle::getUserTransactionId ) );
        Set<KernelTransactionHandle> handles = directDependencies.get( handle );
        if ( handles != null )
        {
            Deque<KernelTransactionHandle> blockerQueue = new ArrayDeque<>( handles );
            while ( !blockerQueue.isEmpty() )
            {
                KernelTransactionHandle transactionHandle = blockerQueue.pop();
                if ( allBlockers.add( transactionHandle ) )
                {
                    Set<KernelTransactionHandle> transactionHandleSet = directDependencies.get( transactionHandle );
                    if ( transactionHandleSet != null )
                    {
                        blockerQueue.addAll( transactionHandleSet );
                    }
                }
            }
        }
        return describe( allBlockers );
    }

    Map<String,Object> describeBlockingLocks( KernelTransactionHandle handle )
    {
        Optional<QuerySnapshot> snapshot = handleSnapshotsMap.get( handle );
        return snapshot.map( QuerySnapshot::resourceInformation ).orElse( Collections.emptyMap() );
    }

    private Map<KernelTransactionHandle,Set<KernelTransactionHandle>> initDirectDependencies()
    {
        Map<KernelTransactionHandle, Set<KernelTransactionHandle>> directDependencies = new HashMap<>();

        Map<KernelTransactionHandle,List<ActiveLock>> transactionLocksMap = handleSnapshotsMap.keySet().stream()
                .collect( toMap( identity(), getTransactionLocks() ) );

        for ( Map.Entry<KernelTransactionHandle,Optional<QuerySnapshot>> entry : handleSnapshotsMap.entrySet() )
        {
            Optional<QuerySnapshot> snapshot = entry.getValue();
            if ( snapshot.isPresent() )
            {
                KernelTransactionHandle txHandle = entry.getKey();
                evaluateDirectDependencies( directDependencies, transactionLocksMap, txHandle, snapshot.get() );
            }
        }
        return directDependencies;
    }

    private static Function<KernelTransactionHandle,List<ActiveLock>> getTransactionLocks()
    {
        return transactionHandle -> transactionHandle.activeLocks().collect( toList() );
    }

    private static void evaluateDirectDependencies( Map<KernelTransactionHandle,Set<KernelTransactionHandle>> directDependencies,
            Map<KernelTransactionHandle,List<ActiveLock>> handleLocksMap, KernelTransactionHandle txHandle, QuerySnapshot querySnapshot )
    {
        List<ActiveLock> waitingOnLocks = querySnapshot.waitingLocks();
        for ( ActiveLock activeLock : waitingOnLocks )
        {
            for ( Map.Entry<KernelTransactionHandle,List<ActiveLock>> handleListEntry : handleLocksMap.entrySet() )
            {
                KernelTransactionHandle kernelTransactionHandle = handleListEntry.getKey();
                if ( !kernelTransactionHandle.equals( txHandle ) )
                {
                    if ( isBlocked( activeLock, handleListEntry.getValue() ) )
                    {
                        Set<KernelTransactionHandle> kernelTransactionHandles =
                                directDependencies.computeIfAbsent( txHandle, handle -> new HashSet<>() );
                        kernelTransactionHandles.add( kernelTransactionHandle );
                    }
                }
            }
        }
    }

    private static boolean isBlocked( ActiveLock activeLock, List<ActiveLock> activeLocks )
    {
        return EXCLUSIVE == activeLock.lockType() ?
               haveAnyLocking( activeLocks, activeLock.resourceType(), activeLock.resourceId() ) :
               haveExclusiveLocking( activeLocks, activeLock.resourceType(), activeLock.resourceId() );
    }

    private static boolean haveAnyLocking( List<ActiveLock> locks, ResourceType resourceType, long resourceId )
    {
        return locks.stream().anyMatch( lock -> lock.resourceId() == resourceId && lock.resourceType() == resourceType );
    }

    private static boolean haveExclusiveLocking( List<ActiveLock> locks, ResourceType resourceType, long resourceId )
    {
        return locks.stream().anyMatch( lock -> EXCLUSIVE == lock.lockType() &&
                lock.resourceId() == resourceId &&
                lock.resourceType() == resourceType );
    }

    private static String describe( Set<KernelTransactionHandle> allBlockers )
    {
        if ( allBlockers.isEmpty() )
        {
            return StringUtils.EMPTY;
        }
        StringJoiner stringJoiner = new StringJoiner( ", ", "[", "]" );
        for ( KernelTransactionHandle blocker : allBlockers )
        {
            stringJoiner.add( blocker.getUserTransactionName() );
        }
        return stringJoiner.toString();
    }
}
