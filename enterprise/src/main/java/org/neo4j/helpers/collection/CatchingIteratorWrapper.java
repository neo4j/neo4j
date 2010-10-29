/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.helpers.collection;

import java.util.Iterator;

public abstract class CatchingIteratorWrapper<T, U> extends PrefetchingIterator<T>
{
    private final Iterator<U> source;

    public CatchingIteratorWrapper( Iterator<U> source )
    {
        this.source = source;
    }

    @Override
    protected T fetchNextOrNull()
    {
        while ( source.hasNext() )
        {
            U nextItem = null;
            try
            {
                nextItem = source.next();
                return underlyingObjectToObject( nextItem );
            }
            catch ( Throwable t )
            {
                if ( exceptionOk( t ) )
                {
                    itemDodged( nextItem );
                    continue;
                }
                if ( t instanceof RuntimeException )
                {
                    throw (RuntimeException) t;
                }
                else if ( t instanceof Error )
                {
                    throw (Error) t;
                }
                throw new RuntimeException( t );
            }
        }
        return null;
    }
    
    protected void itemDodged( U item )
    {
    }

    protected boolean exceptionOk( Throwable t )
    {
        return true;
    }

    protected abstract T underlyingObjectToObject( U object );
}
