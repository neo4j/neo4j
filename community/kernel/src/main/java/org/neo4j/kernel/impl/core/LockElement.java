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
package org.neo4j.kernel.impl.core;

import javax.transaction.Transaction;

import org.neo4j.graphdb.Lock;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockType;

public class LockElement implements Lock
{
    private Object resource;
    private final LockType lockType;
    private final LockManager lockManager;

    public LockElement( Object resource, LockType type, LockManager lockManager )
    {
        if ( resource == null )
            throw new IllegalArgumentException( "Null resource" );
        this.resource = resource;
        this.lockType = type;
        this.lockManager = lockManager;
    }
    
    private boolean released()
    {
        return resource == null;
    }

    public boolean releaseIfAcquired()
    {
        // Assume that we are in a tx context, and will be able 
        // to figure out the tx when we actually end up needing it. 
        return releaseIfAcquired( null );
    }

    public boolean releaseIfAcquired( Transaction tx )
    {
        if ( released() )
            return false;
        lockType.release( lockManager, resource, tx );
        resource = null;
        return true;
    }

    @Override
    public void release()
    {
        if ( !releaseIfAcquired() )
        {
            throw new IllegalStateException( "Already released" );
        }
    }
    
    @Override
    public String toString()
    {
        StringBuilder string = new StringBuilder( lockType.name() ).append( "-LockElement[" );
        if ( released() )
            string.append( "released," );
        string.append( resource );
        return string.append( ']' ).toString();
    }
}
