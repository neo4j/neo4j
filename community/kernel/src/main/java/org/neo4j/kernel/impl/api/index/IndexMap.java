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
package org.neo4j.kernel.impl.api.index;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.function.BiConsumer;
import org.neo4j.kernel.api.index.IndexDescriptor;

/**
 * Bundles various mappings to IndexProxy. Used by IndexingService via IndexMapReference.
 *
 * IndexingService is expected to either make a copy before making any changes or update this
 * while being single threaded.
 */
public final class IndexMap implements Cloneable
{
    private final Map<Long, IndexProxy> indexesById;
    private final Map<IndexDescriptor, IndexProxy> indexesByDescriptor;

    public IndexMap()
    {
        this( new HashMap<Long, IndexProxy>(), new HashMap<IndexDescriptor, IndexProxy>() );
    }

    private IndexMap( Map<Long, IndexProxy> indexesById, Map<IndexDescriptor, IndexProxy> indexesByDescriptor )
    {
        this.indexesById = indexesById;
        this.indexesByDescriptor = indexesByDescriptor;
    }

    public IndexProxy getIndexProxy( long indexId )
    {
        return indexesById.get( indexId );
    }

    public IndexProxy getIndexProxy( IndexDescriptor descriptor )
    {
        return indexesByDescriptor.get( descriptor );
    }

    public void putIndexProxy( long indexId, IndexProxy indexProxy )
    {
        indexesById.put( indexId, indexProxy );
        indexesByDescriptor.put( indexProxy.getDescriptor(), indexProxy );
    }

    public IndexProxy removeIndexProxy( long indexId )
    {
        IndexProxy removedProxy = indexesById.remove( indexId );
        if ( null != removedProxy )
        {
            indexesByDescriptor.remove( removedProxy.getDescriptor() );
        }
        return removedProxy;
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
        return new IndexMap( cloneMap( indexesById ), cloneMap( indexesByDescriptor ) );
    }

    private <K, V> Map<K, V> cloneMap( Map<K, V> map )
    {
        Map<K, V> shallowCopy = new HashMap<>( map.size() );
        shallowCopy.putAll( map );
        return shallowCopy;
    }

    public Iterator<IndexDescriptor> descriptors()
    {
        return indexesByDescriptor.keySet().iterator();
    }

    public int size()
    {
        return indexesById.size();
    }
}
