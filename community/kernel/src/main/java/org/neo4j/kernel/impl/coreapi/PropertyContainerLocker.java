/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.coreapi;

import org.neo4j.function.Supplier;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;

/**
 * Manages user-facing locks. Takes a statement and will close it once the lock has been released.
 */
public class PropertyContainerLocker
{
    public Lock exclusiveLock( Supplier<Statement> stmtSupplier, PropertyContainer container )
    {
        try(Statement statement = stmtSupplier.get())
        {
            if(container instanceof Node )
            {
                statement.readOperations().acquireExclusive( ResourceTypes.NODE, ((Node) container).getId() );
                return new CoreAPILock(stmtSupplier, ResourceTypes.NODE, ((Node) container).getId())
                {
                    @Override
                    void release( Statement statement, Locks.ResourceType type, long resourceId )
                    {
                        statement.readOperations().releaseExclusive( type, resourceId );
                    }
                };
            }
            else if(container instanceof Relationship )
            {
                statement.readOperations().acquireExclusive( ResourceTypes.RELATIONSHIP, ((Relationship) container).getId() );
                return new CoreAPILock(stmtSupplier, ResourceTypes.RELATIONSHIP, ((Relationship) container).getId())
                {
                    @Override
                    void release( Statement statement, Locks.ResourceType type, long resourceId )
                    {
                        statement.readOperations().releaseExclusive( type, resourceId );
                    }
                };
            }
            else
            {
                throw new UnsupportedOperationException( "Only relationships and nodes can be locked." );
            }
        }
    }

    public Lock sharedLock( Supplier<Statement> stmtProvider, PropertyContainer container )
    {
        try(Statement statement = stmtProvider.get())
        {
            if(container instanceof Node )
            {
                statement.readOperations().acquireShared( ResourceTypes.NODE, ((Node) container).getId() );
                return new CoreAPILock(stmtProvider, ResourceTypes.NODE, ((Node) container).getId())
                {
                    @Override
                    void release( Statement statement, Locks.ResourceType type, long resourceId )
                    {
                        statement.readOperations().releaseShared( type, resourceId );
                    }
                };
            }
            else if(container instanceof Relationship )
            {
                statement.readOperations().acquireShared( ResourceTypes.RELATIONSHIP, ((Relationship) container).getId() );
                return new CoreAPILock(stmtProvider, ResourceTypes.RELATIONSHIP, ((Relationship) container).getId())
                {
                    @Override
                    void release( Statement statement, Locks.ResourceType type, long resourceId )
                    {
                        statement.readOperations().releaseShared( type, resourceId );
                    }
                };
            }
            else
            {
                throw new UnsupportedOperationException( "Only relationships and nodes can be locked." );
            }
        }
    }

    private static abstract class CoreAPILock implements Lock
    {
        private final Supplier<Statement> stmtProvider;
        private final Locks.ResourceType type;
        private final long resourceId;
        private boolean released = false;

        public CoreAPILock( Supplier<Statement> stmtProvider, Locks.ResourceType type, long resourceId )
        {
            this.stmtProvider = stmtProvider;
            this.type = type;
            this.resourceId = resourceId;
        }

        @Override
        public void release()
        {
            if( released )
            {
                throw new IllegalStateException( "Already released" );
            }
            released = true;
            try(Statement statement = stmtProvider.get())
            {
                release( statement, type, resourceId );
            }
        }

        abstract void release( Statement statement, Locks.ResourceType type, long resourceId );
    }

}
