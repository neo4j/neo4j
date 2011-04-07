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

import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.RelIdIterator;

class FastRelTypeElement extends RelTypeElementIterator
{
    private final RelIdArray src;

    private final RelIdIterator iterator;

    FastRelTypeElement( String type, NodeImpl node, RelIdArray src )
    {
        super( type, node );
        this.src = src == null ? RelIdArray.EMPTY : src;
        this.iterator = this.src.iterator();
    }
    
    FastRelTypeElement( String type, NodeImpl node, RelIdArray src, int position )
    {
        this( type, node, src );
        this.iterator.fastForwardTo( position );
    }

    @Override
    public boolean hasNext( NodeManager nodeManager )
    {
        return this.iterator.hasNext();
    }

    @Override
    public long next( NodeManager nodeManager )
    {
        return this.iterator.next();
    }
    
    @Override
    public boolean isSrcEmpty()
    {
        return src.isEmpty();
    }

    @Override
    public RelTypeElementIterator setSrc( RelIdArray newSrc )
    {
        int position = iterator.position();
        return new FastRelTypeElement( getType(), getNode(), newSrc, position );
    }
}
