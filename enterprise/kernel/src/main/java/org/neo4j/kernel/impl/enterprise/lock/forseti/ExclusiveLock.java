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
package org.neo4j.kernel.impl.enterprise.lock.forseti;

import java.util.Set;

import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

class ExclusiveLock implements ForsetiLockManager.Lock
{
    private final ForsetiClient owner;

    ExclusiveLock( ForsetiClient owner )
    {
        this.owner = owner;
    }

    @Override
    public void copyHolderWaitListsInto( SimpleBitSet waitList )
    {
        owner.copyWaitListTo( waitList );
    }

    @Override
    public int detectDeadlock( int client )
    {
        return owner.isWaitingFor( client ) ? owner.id() : -1;
    }

    @Override
    public String describeWaitList()
    {
        return "ExclusiveLock[" + owner.describeWaitList() + "]";
    }

    @Override
    public void collectOwners( Set<ForsetiClient> owners )
    {
        owners.add( owner );
    }

    @Override
    public String toString()
    {
        return "ExclusiveLock{" +
               "owner=" + owner +
               '}';
    }
}
