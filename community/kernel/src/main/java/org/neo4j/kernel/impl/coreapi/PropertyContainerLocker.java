/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.coreapi;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;

/**
 * Manages user-facing locks.
 */
public class PropertyContainerLocker
{
    public Lock exclusiveLock( KernelTransaction ktx, PropertyContainer container )
    {
        try ( Statement ignore = ktx.acquireStatement() )
        {
            if ( container instanceof Node )
            {
                long id = ((Node) container).getId();
                ktx.locks().acquireExclusiveNodeLock( id );
                return new CoreAPILock( () -> ktx.locks().releaseExclusiveNodeLock( id ) );
            }
            else if ( container instanceof Relationship )
            {
                long id = ((Relationship) container).getId();
                ktx.locks().acquireExclusiveRelationshipLock( id );
                return new CoreAPILock( () -> ktx.locks().releaseExclusiveRelationshipLock( id ) );
            }
            else
            {
                throw new UnsupportedOperationException( "Only relationships and nodes can be locked." );
            }
        }
    }

    public Lock sharedLock( KernelTransaction ktx, PropertyContainer container )
    {
        try ( Statement ignore = ktx.acquireStatement() )
        {
            if ( container instanceof Node )
            {
                long id = ((Node) container).getId();
                ktx.locks().acquireSharedNodeLock( id );
                return new CoreAPILock( () -> ktx.locks().releaseSharedNodeLock( id ) );
            }
            else if ( container instanceof Relationship )
            {
                long id = ((Relationship) container).getId();
                ktx.locks().acquireSharedRelationshipLock( id );
                return new CoreAPILock( () -> ktx.locks().releaseSharedRelationshipLock( id ) );
            }
            else
            {
                throw new UnsupportedOperationException( "Only relationships and nodes can be locked." );
            }
        }
    }

    private static class CoreAPILock implements Lock
    {
        private boolean released;
        private final Runnable release;

        CoreAPILock( Runnable release )
        {
            this.release = release;
        }

        @Override
        public void release()
        {
            if ( released )
            {
                throw new IllegalStateException( "Already released" );
            }
            released = true;
            release.run();
        }
    }

}
