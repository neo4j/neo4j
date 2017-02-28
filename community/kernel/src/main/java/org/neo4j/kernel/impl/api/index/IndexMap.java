/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.function.BiConsumer;

import org.neo4j.kernel.api.schema.IndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.IndexBoundary;

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
    private final Map<IndexDescriptor, Long> indexIdsByDescriptor;

    public IndexMap()
    {
        this( new HashMap<>(), new HashMap<>(), new HashMap<>() );
    }

    private IndexMap( Map<Long, IndexProxy> indexesById, Map<IndexDescriptor, IndexProxy> indexesByDescriptor, Map<IndexDescriptor, Long> indexIdsByDescriptor )
    {
        this.indexesById = indexesById;
        this.indexesByDescriptor = indexesByDescriptor;
        this.indexIdsByDescriptor = indexIdsByDescriptor;
    }

    public IndexProxy getIndexProxy( long indexId )
    {
        return indexesById.get( indexId );
    }

    public IndexProxy getIndexProxy( IndexDescriptor descriptor )
    {
        return indexesByDescriptor.get( descriptor );
    }

    public long getIndexId( IndexDescriptor descriptor )
    {
        return indexIdsByDescriptor.get( descriptor );
    }

    public void putIndexProxy( long indexId, IndexProxy indexProxy )
    {
        indexesById.put( indexId, indexProxy );
        indexesByDescriptor.put( IndexBoundary.map( indexProxy.getDescriptor() ), indexProxy );
        indexIdsByDescriptor.put( IndexBoundary.map( indexProxy.getDescriptor() ), indexId );
    }

    public IndexProxy removeIndexProxy( long indexId )
    {
        IndexProxy removedProxy = indexesById.remove( indexId );
        if ( null != removedProxy )
        {
            indexesByDescriptor.remove( IndexBoundary.map( removedProxy.getDescriptor() ) );
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
        return new IndexMap( cloneMap( indexesById ), cloneMap( indexesByDescriptor ),
                cloneMap( indexIdsByDescriptor ) );
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

    public Iterator<Long> indexIds()
    {
        return indexesById.keySet().iterator();
    }

    public int size()
    {
        return indexesById.size();
    }
}
