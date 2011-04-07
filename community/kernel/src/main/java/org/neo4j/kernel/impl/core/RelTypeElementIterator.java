/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.util.NoSuchElementException;

import org.neo4j.kernel.impl.util.RelIdArray;

public abstract class RelTypeElementIterator
{
    protected static RelTypeElementIterator EMPTY = new RelTypeElementIterator( null, null )
    {
        @Override
        public RelTypeElementIterator setSrc( RelIdArray newSrc )
        {
            return this;
        }
        
        @Override
        public long next( NodeManager nodeManager )
        {
            throw new NoSuchElementException();
        }
        
        @Override
        public boolean isSrcEmpty()
        {
            return true;
        }
        
        @Override
        public boolean hasNext( NodeManager nodeManager )
        {
            return false;
        }
    };
    
    private final String type;
    private final NodeImpl node;
    
    RelTypeElementIterator( String type, NodeImpl node )
    {
        this.type = type;
        this.node = node;
    }
    
    NodeImpl getNode()
    {
        return node;
    }
    
    public String getType()
    {
        return type;
    }

    public abstract boolean hasNext( NodeManager nodeManager );

    public abstract long next( NodeManager nodeManager );
    
    public abstract boolean isSrcEmpty();

    public abstract RelTypeElementIterator setSrc( RelIdArray newSrc );
}
