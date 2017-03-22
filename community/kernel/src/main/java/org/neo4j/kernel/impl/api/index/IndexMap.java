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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;

/**
 * Bundles various mappings to IndexProxy. Used by IndexingService via IndexMapReference.
 *
 * IndexingService is expected to either make a copy before making any changes or update this
 * while being single threaded.
 */
public final class IndexMap implements Cloneable
{
    private final Map<Long, IndexProxy> indexesById;
    private final Map<LabelSchemaDescriptor,IndexProxy> indexesByDescriptor;
    private final Map<LabelSchemaDescriptor,Long> indexIdsByDescriptor;
    private final PrimitiveLongObjectMap<Set<LabelSchemaDescriptor>> descriptorsByLabel;

    public IndexMap()
    {
        this( new HashMap<>(), new HashMap<>(), new HashMap<>() );
    }

    private IndexMap(
            Map<Long, IndexProxy> indexesById,
            Map<LabelSchemaDescriptor,IndexProxy> indexesByDescriptor,
            Map<LabelSchemaDescriptor,Long> indexIdsByDescriptor )
    {
        this.indexesById = indexesById;
        this.indexesByDescriptor = indexesByDescriptor;
        this.indexIdsByDescriptor = indexIdsByDescriptor;
        this.descriptorsByLabel = Primitive.longObjectMap();
        for ( LabelSchemaDescriptor schema : indexesByDescriptor.keySet() )
        {
            addDescriptorToLabelLookup( schema );
        }
    }

    public IndexProxy getIndexProxy( long indexId )
    {
        return indexesById.get( indexId );
    }

    public IndexProxy getIndexProxy( LabelSchemaDescriptor descriptor )
    {
        return indexesByDescriptor.get( descriptor );
    }

    public long getIndexId( LabelSchemaDescriptor descriptor )
    {
        return indexIdsByDescriptor.get( descriptor );
    }

    public void putIndexProxy( long indexId, IndexProxy indexProxy )
    {
        LabelSchemaDescriptor schema = indexProxy.getDescriptor().schema();
        indexesById.put( indexId, indexProxy );
        indexesByDescriptor.put( schema, indexProxy );
        indexIdsByDescriptor.put( schema, indexId );
        addDescriptorToLabelLookup( schema );
    }

    public IndexProxy removeIndexProxy( long indexId )
    {
        IndexProxy removedProxy = indexesById.remove( indexId );
        if ( removedProxy == null )
        {
            return null;
        }

        LabelSchemaDescriptor schema = removedProxy.getDescriptor().schema();
        indexesByDescriptor.remove( schema );

        Set<LabelSchemaDescriptor> descriptors = descriptorsByLabel.get( schema.getLabelId() );
        descriptors.remove( schema );
        if ( descriptors.isEmpty() )
        {
            descriptorsByLabel.remove( schema.getLabelId() );
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

    public Iterator<LabelSchemaDescriptor> descriptors()
    {
        return indexesByDescriptor.keySet().iterator();
    }

    public Iterator<LabelSchemaDescriptor> descriptorsForLabels( long[] labels )
    {
        if ( labels.length == 1 )
        {
            return descriptorsByLabel.get( labels[0] ).iterator();
        }

        List<LabelSchemaDescriptor> descriptors = new ArrayList<>();
        for ( long label : labels )
        {
            descriptors.addAll( descriptorsByLabel.get( label ) );
        }
        return descriptors.iterator();
    }

    public Iterator<Long> indexIds()
    {
        return indexesById.keySet().iterator();
    }

    public int size()
    {
        return indexesById.size();
    }

    private void addDescriptorToLabelLookup( LabelSchemaDescriptor schema )
    {
        if ( !descriptorsByLabel.containsKey( schema.getLabelId() ) )
        {
            descriptorsByLabel.put( schema.getLabelId(), new HashSet<>() );
        }
        descriptorsByLabel.get( schema.getLabelId() ).add( schema );
    }
}
