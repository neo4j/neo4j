/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi.gdsimpl;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class GDSBackedLocker implements RepeatableReadQueryContext.Locker
{
    private final Transaction transaction;
    private final Map<Long, Lock> nodeLocks = new HashMap<Long, Lock>();
    private final Map<Long, Lock> relationshipLocks = new HashMap<Long, Lock>();

    public GDSBackedLocker( Transaction transaction )
    {
        this.transaction = transaction;
    }

    @Override
    public void readLock( PropertyContainer pc )
    {
        if ( pc instanceof Node )
        {
            lock( pc, ((Node) pc).getId(), nodeLocks );
        } else
        {
            lock( pc, ((Relationship) pc).getId(), relationshipLocks );
        }
    }

    private void lock( PropertyContainer pc, long id, Map<Long, Lock> lockHolder )
    {
        if ( !lockHolder.containsKey( id ) )
        {
            Lock lock = transaction.acquireReadLock( pc );
            relationshipLocks.put( id, lock );
        }
    }

    @Override
    public void releaseAllReadLocks()
    {
        for(Lock lock : nodeLocks.values())
        {
            lock.release();
        }

        for(Lock lock : relationshipLocks.values())
        {
            lock.release();
        }
    }
}
