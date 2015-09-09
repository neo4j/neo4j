/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.locking.community;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntObjectVisitor;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongObjectVisitor;
import org.neo4j.kernel.impl.locking.Locks;

import static java.lang.String.format;

public class CommunityLockClient implements Locks.Client
{
    private final LockManagerImpl manager;
    private final LockTransaction lockTransaction = new LockTransaction();

    private final PrimitiveIntObjectMap<PrimitiveLongObjectMap<LockResource>> sharedLocks = Primitive.intObjectMap();
    private final PrimitiveIntObjectMap<PrimitiveLongObjectMap<LockResource>> exclusiveLocks = Primitive.intObjectMap();

    public CommunityLockClient( LockManagerImpl manager )
    {
        this.manager = manager;
    }

    @Override
    public void acquireShared( Locks.ResourceType resourceType, long... resourceIds )
    {
        PrimitiveLongObjectMap<LockResource> localLocks = localShared( resourceType );
        for ( long resourceId : resourceIds )
        {
            LockResource resource = localLocks.get( resourceId );
            if( resource != null )
            {
                resource.acquireReference();
                continue;
            }

            resource = new LockResource( resourceType, resourceId );
            manager.getReadLock( resource, lockTransaction );
            localLocks.put(resourceId, resource);
        }
    }

    @Override
    public void acquireExclusive( Locks.ResourceType resourceType, long... resourceIds )
    {
        PrimitiveLongObjectMap<LockResource> localLocks = localExclusive( resourceType );
        for ( long resourceId : resourceIds )
        {
            LockResource resource = localLocks.get( resourceId );
            if( resource != null )
            {
                resource.acquireReference();
                continue;
            }

            resource = new LockResource( resourceType, resourceId );
            manager.getWriteLock( resource, lockTransaction );
            localLocks.put(resourceId, resource);
        }
    }

    @Override
    public boolean tryExclusiveLock( Locks.ResourceType resourceType, long... resourceIds )
    {
        PrimitiveLongObjectMap<LockResource> localLocks = localExclusive( resourceType );
        for ( long resourceId : resourceIds )
        {
            LockResource resource = localLocks.get( resourceId );
            if( resource != null )
            {
                resource.acquireReference();
                continue;
            }

            resource = new LockResource( resourceType, resourceId );
            if(manager.tryWriteLock( resource, lockTransaction ))
            {
                localLocks.put(resourceId, resource);
            }
            else
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean trySharedLock( Locks.ResourceType resourceType, long... resourceIds )
    {
        PrimitiveLongObjectMap<LockResource> localLocks = localShared( resourceType );
        for ( long resourceId : resourceIds )
        {
            LockResource resource = localLocks.get( resourceId );
            if( resource != null )
            {
                resource.acquireReference();
                continue;
            }

            resource = new LockResource( resourceType, resourceId );
            if(manager.tryReadLock( resource, lockTransaction ))
            {
                localLocks.put(resourceId, resource);
            }
            else
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void releaseShared( Locks.ResourceType resourceType, long... resourceIds )
    {
        PrimitiveLongObjectMap<LockResource> localLocks = localShared( resourceType );
        for ( long resourceId : resourceIds )
        {
            LockResource resource = localLocks.get( resourceId );
            if( resource.releaseReference() != 0)
            {
                continue;
            }
            localLocks.remove( resourceId );

            manager.releaseReadLock( new LockResource( resourceType, resourceId ), lockTransaction );
        }
    }

    @Override
    public void releaseExclusive( Locks.ResourceType resourceType, long... resourceIds )
    {
        PrimitiveLongObjectMap<LockResource> localLocks = localExclusive( resourceType );
        for ( long resourceId : resourceIds )
        {
            LockResource resource = localLocks.get( resourceId );
            if( resource.releaseReference() != 0)
            {
                continue;
            }
            localLocks.remove( resourceId );

            manager.releaseWriteLock( new LockResource( resourceType, resourceId ), lockTransaction );
        }
    }

    @Override
    public void releaseAllShared()
    {
        sharedLocks.visitEntries( typeReadReleaser );
        sharedLocks.clear();
    }

    @Override
    public void releaseAllExclusive()
    {
        exclusiveLocks.visitEntries( typeWriteReleaser );
        exclusiveLocks.clear();
    }

    @Override
    public void releaseAll()
    {
        releaseAllExclusive();
        releaseAllShared();
    }

    @Override
    public void close()
    {
        releaseAll();
    }

    @Override
    public int getLockSessionId()
    {
        return lockTransaction.getId();
    }

    private PrimitiveLongObjectMap<LockResource> localShared( Locks.ResourceType resourceType )
    {
        PrimitiveLongObjectMap<LockResource> map = sharedLocks.get( resourceType.typeId() );
        if(map == null)
        {
            map = Primitive.longObjectMap();
            sharedLocks.put( resourceType.typeId(), map );
        }
        return map;
    }

    private PrimitiveLongObjectMap<LockResource> localExclusive( Locks.ResourceType resourceType )
    {
        PrimitiveLongObjectMap<LockResource> map = exclusiveLocks.get( resourceType.typeId() );
        if(map == null)
        {
            map = Primitive.longObjectMap();
            exclusiveLocks.put( resourceType.typeId(), map );
        }
        return map;
    }

    private final PrimitiveIntObjectVisitor<PrimitiveLongObjectMap<LockResource>, RuntimeException> typeReadReleaser = new
            PrimitiveIntObjectVisitor<PrimitiveLongObjectMap<LockResource>, RuntimeException>()
    {
        @Override
        public boolean visited( int key, PrimitiveLongObjectMap<LockResource> value ) throws RuntimeException
        {
            value.visitEntries( readReleaser );
            return false;
        }
    };

    private final PrimitiveIntObjectVisitor<PrimitiveLongObjectMap<LockResource>, RuntimeException> typeWriteReleaser = new
            PrimitiveIntObjectVisitor<PrimitiveLongObjectMap<LockResource>, RuntimeException>()
    {
        @Override
        public boolean visited( int key, PrimitiveLongObjectMap<LockResource> value ) throws RuntimeException
        {
            value.visitEntries( writeReleaser );
            return false;
        }
    };

    private final PrimitiveLongObjectVisitor<LockResource, RuntimeException> writeReleaser = new PrimitiveLongObjectVisitor<LockResource, RuntimeException>()
    {
        @Override
        public boolean visited( long key, LockResource lockResource ) throws RuntimeException
        {
            manager.releaseWriteLock( lockResource, lockTransaction );
            return false;
        }
    };

    private final PrimitiveLongObjectVisitor<LockResource, RuntimeException> readReleaser = new PrimitiveLongObjectVisitor<LockResource, RuntimeException>()
    {
        @Override
        public boolean visited( long key, LockResource lockResource ) throws RuntimeException
        {
            manager.releaseReadLock( lockResource, lockTransaction );
            return false;
        }
    };

    @Override
    public String toString()
    {
        return format( "%s[%d]", getClass().getSimpleName(), getLockSessionId() );
    }
}
