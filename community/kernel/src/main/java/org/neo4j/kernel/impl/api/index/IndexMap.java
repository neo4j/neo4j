/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.eclipse.collections.api.block.procedure.primitive.LongObjectProcedure;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.internal.schema.IndexDescriptor;

/**
 * Bundles various mappings to IndexProxy. Used by IndexingService via IndexMapReference.
 *
 * IndexingService is expected to either make a copy before making any changes or update this
 * while being single threaded.
 */
public final class IndexMap implements Cloneable
{
    private final MutableLongObjectMap<IndexProxy> indexesById;
    private final Set<IndexDescriptor> indexDescriptors;

    public IndexMap()
    {
        this( new LongObjectHashMap<>(), new HashSet<>() );
    }

    private IndexMap(
            MutableLongObjectMap<IndexProxy> indexesById,
            Set<IndexDescriptor> indexesByDescriptor )
    {
        this.indexesById = indexesById;
        this.indexDescriptors = indexesByDescriptor;
    }

    public IndexProxy getIndexProxy( IndexDescriptor index )
    {
        return indexesById.get( index.getId() );
    }

    public IndexProxy getIndexProxy( long indexId )
    {
        return indexesById.get( indexId );
    }

    public void putIndexProxy( IndexProxy indexProxy )
    {
        IndexDescriptor index = indexProxy.getDescriptor();
        indexesById.put( index.getId(), indexProxy );
        indexDescriptors.add( index );
    }

    IndexProxy removeIndexProxy( long indexId )
    {
        IndexProxy removedProxy = indexesById.remove( indexId );
        if ( removedProxy == null )
        {
            return null;
        }

        IndexDescriptor index = removedProxy.getDescriptor();
        indexDescriptors.remove( index );
        return removedProxy;
    }

    void forEachIndexProxy( LongObjectProcedure<IndexProxy> consumer )
    {
        indexesById.forEachKeyValue( consumer );
    }

    Iterable<IndexProxy> getAllIndexProxies()
    {
        return indexesById.values();
    }

    @Override
    public IndexMap clone()
    {
        return new IndexMap( LongObjectHashMap.newMap( indexesById ), new HashSet<>( indexDescriptors ) );
    }

    public Iterator<IndexDescriptor> descriptors()
    {
        return indexDescriptors.iterator();
    }

    public LongIterator indexIds()
    {
        return indexesById.keySet().longIterator();
    }

    public int size()
    {
        return indexesById.size();
    }
}
