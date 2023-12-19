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
package org.neo4j.kernel.enterprise.builtinprocs;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.Function;

import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.storageengine.api.lock.ResourceType;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class TransactionDependenciesResolver
{
    private final Map<KernelTransactionHandle,List<QuerySnapshot>> handleSnapshotsMap;
    private Map<KernelTransactionHandle,Set<KernelTransactionHandle>> directDependencies;

    TransactionDependenciesResolver( Map<KernelTransactionHandle,List<QuerySnapshot>> handleSnapshotsMap )
    {
        this.handleSnapshotsMap = handleSnapshotsMap;
        this.directDependencies = initDirectDependencies();
    }

    public boolean isBlocked( KernelTransactionHandle handle )
    {
        return directDependencies.get( handle ) != null;
    }

    public String describeBlockingTransactions( KernelTransactionHandle handle  )
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

    public Map<String,Object> describeBlockingLocks( KernelTransactionHandle handle )
    {
        List<QuerySnapshot> querySnapshots = handleSnapshotsMap.get( handle );
        if ( !querySnapshots.isEmpty() )
        {
            return querySnapshots.get( 0 ).resourceInformation();
        }
        return Collections.emptyMap();
    }

    private Map<KernelTransactionHandle,Set<KernelTransactionHandle>> initDirectDependencies()
    {
        Map<KernelTransactionHandle, Set<KernelTransactionHandle>> directDependencies = new HashMap<>();

        Map<KernelTransactionHandle,List<ActiveLock>> transactionLocksMap = handleSnapshotsMap.keySet().stream()
                .collect( toMap( identity(), getTransactionLocks() ) );

        for ( Map.Entry<KernelTransactionHandle,List<QuerySnapshot>> entry : handleSnapshotsMap.entrySet() )
        {
            List<QuerySnapshot> querySnapshots = entry.getValue();
            if ( !querySnapshots.isEmpty() )
            {
                KernelTransactionHandle txHandle = entry.getKey();
                evaluateDirectDependencies( directDependencies, transactionLocksMap, txHandle, querySnapshots.get( 0 ) );
            }
        }
        return directDependencies;
    }

    private Function<KernelTransactionHandle,List<ActiveLock>> getTransactionLocks()
    {
        return transactionHandle -> transactionHandle.activeLocks().collect( toList() );
    }

    private void evaluateDirectDependencies( Map<KernelTransactionHandle,Set<KernelTransactionHandle>> directDependencies,
            Map<KernelTransactionHandle,List<ActiveLock>> handleLocksMap, KernelTransactionHandle txHandle,
            QuerySnapshot querySnapshot )
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

    private boolean isBlocked( ActiveLock activeLock, List<ActiveLock> activeLocks )
    {
        return ActiveLock.EXCLUSIVE_MODE.equals( activeLock.mode() ) ?
               haveAnyLocking( activeLocks, activeLock.resourceType(), activeLock.resourceId() ) :
               haveExclusiveLocking( activeLocks, activeLock.resourceType(), activeLock.resourceId() );
    }

    private static boolean haveAnyLocking( List<ActiveLock> locks, ResourceType resourceType, long resourceId )
    {
        return locks.stream().anyMatch( lock -> lock.resourceId() == resourceId && lock.resourceType() == resourceType );
    }

    private static boolean haveExclusiveLocking( List<ActiveLock> locks, ResourceType resourceType, long resourceId )
    {
        return locks.stream().anyMatch( lock -> ActiveLock.EXCLUSIVE_MODE.equals( lock.mode() ) &&
                lock.resourceId() == resourceId &&
                lock.resourceType() == resourceType );
    }

    private String describe( Set<KernelTransactionHandle> allBlockers )
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
