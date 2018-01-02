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
package org.neo4j.kernel.impl.locking;

/**
 * Abstract implementation to keep all the boiler-plate code separate from actual locking logic.
 *
 * Diagram of how classes inter-relate:
 * <pre>
 * ({@link LockService}) --------[returns]--------> ({@link Lock})
 *  ^                                            ^
 *  |                                            |
 *  [implements]                                 [extends]
 *  |                                            |
 * ({@link AbstractLockService}) -[contains]-> ({@link LockReference}) -[holds]-> ({@link LockedEntity})
 *  ^      |                                     |                                     ^
 *  |      |                                     [references]                          |
 *  |      |                                     |                                     [extends]
 *  |      |                                     V                                     |
 *  |      +-----------[type param]-----------> (HANDLE)                              ({@link LockedNode})
 *  |                                            ^
 *  [extends]                                    |
 *  |                                            [satisfies]
 *  |                                            |
 * ({@link ReentrantLockService})-[type param]->({@link ReentrantLockService.OwnerQueueElement})
 * </pre>
 *
 * @param <HANDLE> A handle that the concrete implementation used for releasing the lock.
 */
abstract class AbstractLockService<HANDLE> implements LockService
{
    @Override
    public Lock acquireNodeLock( long nodeId, LockType type )
    {
        return lock( new LockedNode( nodeId ) );
    }

    @Override
    public Lock acquireRelationshipLock( long relationshipId, LockType type )
    {
        return lock( new LockedRelationship( relationshipId ) );
    }

    private Lock lock( LockedEntity key )
    {
        return new LockReference( key, acquire( key ) );
    }

    protected abstract HANDLE acquire( LockedEntity key );

    protected abstract void release( LockedEntity key, HANDLE handle );

    protected static abstract class LockedEntity
    {
        private LockedEntity()
        {
            // all instances defined in this class
        }

        @Override
        public final String toString()
        {
            StringBuilder repr = new StringBuilder( getClass().getSimpleName() ).append( '[' );
            toString( repr );
            return repr.append( ']' ).toString();
        }

        abstract void toString( StringBuilder repr );

        @Override
        public abstract int hashCode();

        @Override
        public abstract boolean equals( Object obj );
    }

    private class LockReference extends Lock
    {
        private final LockedEntity key;
        private HANDLE handle;

        LockReference( LockedEntity key, HANDLE handle )
        {
            this.key = key;
            this.handle = handle;
        }

        @Override
        public String toString()
        {
            StringBuilder repr = new StringBuilder( key.getClass().getSimpleName() ).append( '[' );
            key.toString( repr );
            if ( handle != null )
            {
                repr.append( "; HELD_BY=" ).append( handle );
            }
            else
            {
                repr.append( "; RELEASED" );
            }
            return repr.append( ']' ).toString();
        }

        @Override
        public void release()
        {
            if ( handle == null )
            {
                return;
            }
            try
            {
                AbstractLockService.this.release( key, handle );
            }
            finally
            {
                handle = null;
            }
        }
    }

    static class LockedPropertyContainer extends LockedEntity
    {
        private final long id;

        LockedPropertyContainer( long id )
        {
            this.id = id;
        }

        @Override
        void toString( StringBuilder repr )
        {
            repr.append( "id=" ).append( id );
        }

        @Override
        public int hashCode()
        {
            return (int) (id ^ (id >>> 32));
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj.getClass().equals( getClass() ) )
            {
                LockedPropertyContainer that = (LockedPropertyContainer) obj;
                return this.id == that.id;
            }
            return false;
        }
    }

    static final class LockedNode extends LockedPropertyContainer
    {
        LockedNode( long nodeId )
        {
            super( nodeId );
        }
    }

    static final class LockedRelationship extends LockedPropertyContainer
    {
        LockedRelationship( long relationshipId )
        {
            super( relationshipId );
        }
    }
}
