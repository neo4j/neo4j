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
package org.neo4j.kernel.impl.api.index;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.BiConsumer;

public final class IndexMap implements Cloneable
{
    private final Map<Long, IndexProxy> indexesById;

    public IndexMap()
    {
        this( new HashMap<Long, IndexProxy>() );
    }

    private IndexMap( Map<Long, IndexProxy> indexesById )
    {
        this.indexesById = indexesById;
    }

    public IndexProxy getIndexProxy( long indexId )
    {
        return indexesById.get( indexId );
    }

    public IndexProxy putIndexProxy( long indexId, IndexProxy indexProxy )
    {
        return indexesById.put( indexId, indexProxy );
    }

    public IndexProxy removeIndexProxy( long indexId )
    {
        return indexesById.remove( indexId );
    }

    public void foreachIndexProxy( BiConsumer<Long, IndexProxy> consumer )
    {
        for ( Map.Entry<Long, IndexProxy> entry : indexesById.entrySet() )
        {
            consumer.accept( entry.getKey(), entry.getValue() );
        }
    }

    public Iterable<IndexProxy> getAllIndexProxies()
    {
        return indexesById.values();
    }

    @Override
    public IndexMap clone()
    {
        return new IndexMap( indexesById );
    }

}
