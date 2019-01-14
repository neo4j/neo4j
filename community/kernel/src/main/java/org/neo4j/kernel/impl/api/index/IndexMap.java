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
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.LongObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.impl.block.factory.Functions;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

/**
 * Bundles various mappings to IndexProxy. Used by IndexingService via IndexMapReference.
 *
 * IndexingService is expected to either make a copy before making any changes or update this
 * while being single threaded.
 */
public final class IndexMap implements Cloneable
{
    private final MutableLongObjectMap<IndexProxy> indexesById;
    private final Map<SchemaDescriptor,IndexProxy> indexesByDescriptor;
    private final MutableObjectLongMap<SchemaDescriptor> indexIdsByDescriptor;
    private final MutableIntObjectMap<Set<SchemaDescriptor>> descriptorsByLabel;
    private final MutableIntObjectMap<Set<SchemaDescriptor>> descriptorsByReltype;
    private final MutableIntObjectMap<Set<SchemaDescriptor>> nodeDescriptorsByProperty;
    private final MutableIntObjectMap<Set<SchemaDescriptor>> relationshipDescriptorsByProperty;

    public IndexMap()
    {
        this( new LongObjectHashMap<>(), new HashMap<>(), new ObjectLongHashMap<>() );
    }

    IndexMap( MutableLongObjectMap<IndexProxy> indexesById )
    {
        this( indexesById, indexesByDescriptor( indexesById ), indexIdsByDescriptor( indexesById ) );
    }

    private IndexMap(
            MutableLongObjectMap<IndexProxy> indexesById,
            Map<SchemaDescriptor,IndexProxy> indexesByDescriptor,
            MutableObjectLongMap<SchemaDescriptor> indexIdsByDescriptor )
    {
        this.indexesById = indexesById;
        this.indexesByDescriptor = indexesByDescriptor;
        this.indexIdsByDescriptor = indexIdsByDescriptor;
        this.descriptorsByLabel = new IntObjectHashMap<>();
        this.descriptorsByReltype = new IntObjectHashMap<>();
        this.nodeDescriptorsByProperty = new IntObjectHashMap<>();
        this.relationshipDescriptorsByProperty = new IntObjectHashMap<>();
        for ( SchemaDescriptor schema : indexesByDescriptor.keySet() )
        {
            addDescriptorToLookups( schema );
        }
    }

    public IndexProxy getIndexProxy( long indexId )
    {
        return indexesById.get( indexId );
    }

    public IndexProxy getIndexProxy( SchemaDescriptor descriptor )
    {
        return indexesByDescriptor.get( descriptor );
    }

    public long getIndexId( SchemaDescriptor descriptor )
    {
        return indexIdsByDescriptor.get( descriptor );
    }

    public void putIndexProxy( IndexProxy indexProxy )
    {
        StoreIndexDescriptor descriptor = indexProxy.getDescriptor();
        SchemaDescriptor schema = descriptor.schema();
        indexesById.put( descriptor.getId(), indexProxy );
        indexesByDescriptor.put( schema, indexProxy );
        indexIdsByDescriptor.put( schema, descriptor.getId() );
        addDescriptorToLookups( schema );
    }

    public IndexProxy removeIndexProxy( long indexId )
    {
        IndexProxy removedProxy = indexesById.remove( indexId );
        if ( removedProxy == null )
        {
            return null;
        }

        SchemaDescriptor schema = removedProxy.getDescriptor().schema();
        indexesByDescriptor.remove( schema );
        if ( schema.entityType() == EntityType.NODE )
        {
            removeFromLookup( schema.getEntityTokenIds(), schema, descriptorsByLabel );
            removeFromLookup( schema.getPropertyIds(), schema, nodeDescriptorsByProperty );
        }
        else if ( schema.entityType() == EntityType.RELATIONSHIP )
        {
            removeFromLookup( schema.getEntityTokenIds(), schema, descriptorsByReltype );
            removeFromLookup( schema.getPropertyIds(), schema, relationshipDescriptorsByProperty );
        }

        return removedProxy;
    }

    void forEachIndexProxy( LongObjectProcedure<IndexProxy> consumer )
    {
        indexesById.forEachKeyValue( consumer );
    }

    public Iterable<IndexProxy> getAllIndexProxies()
    {
        return indexesById.values();
    }

    /**
     * Get all indexes that would be affected by changes in the input labels and/or properties. The returned
     * indexes are guaranteed to contain all affected indexes, but might also contain unaffected indexes as
     * we cannot provide matching without checking unaffected properties for composite indexes.
     *
     * @param changedEntityTokens set of labels that have changed
     * @param unchangedEntityTokens set of labels that are unchanged
     * @param properties set of properties
     * @param entityType type of indexes to get
     * @return set of SchemaDescriptors describing the potentially affected indexes
     */
    public Set<SchemaDescriptor> getRelatedIndexes( long[] changedEntityTokens, long[] unchangedEntityTokens, IntSet properties, EntityType entityType )
    {
        switch ( entityType )
        {
        case NODE:
            return getRelatedDescriptors( changedEntityTokens, unchangedEntityTokens, properties, descriptorsByLabel, nodeDescriptorsByProperty );
        case RELATIONSHIP:
            return getRelatedDescriptors( changedEntityTokens, unchangedEntityTokens, properties, descriptorsByReltype, relationshipDescriptorsByProperty );
        default:
            throw new IllegalArgumentException( "The given EntityType cannot be indexed: " + entityType );
        }
    }

    @Override
    public IndexMap clone()
    {
        return new IndexMap( LongObjectHashMap.newMap( indexesById ), cloneMap( indexesByDescriptor ), new ObjectLongHashMap<>( indexIdsByDescriptor ) );
    }

    public Iterator<SchemaDescriptor> descriptors()
    {
        return indexesByDescriptor.keySet().iterator();
    }

    public LongIterator indexIds()
    {
        return indexesById.keySet().longIterator();
    }

    public int size()
    {
        return indexesById.size();
    }

    // HELPERS

    private <K, V> Map<K, V> cloneMap( Map<K, V> map )
    {
        Map<K, V> shallowCopy = new HashMap<>( map.size() );
        shallowCopy.putAll( map );
        return shallowCopy;
    }

    private void addDescriptorToLookups( SchemaDescriptor schema )
    {
        if ( schema.entityType() == EntityType.NODE )
        {
            for ( int entityTokenId : schema.getEntityTokenIds() )
            {
                addToLookup( entityTokenId, schema, descriptorsByLabel );
            }
            for ( int propertyId : schema.getPropertyIds() )
            {
                addToLookup( propertyId, schema, nodeDescriptorsByProperty );
            }
        }
        else if ( schema.entityType() == EntityType.RELATIONSHIP )
        {
            for ( int entityTokenId : schema.getEntityTokenIds() )
            {
                addToLookup( entityTokenId, schema, descriptorsByReltype );
            }
            for ( int propertyId : schema.getPropertyIds() )
            {
                addToLookup( propertyId, schema, relationshipDescriptorsByProperty );
            }
        }
    }

    private void addToLookup(
            int key,
            SchemaDescriptor schema,
            MutableIntObjectMap<Set<SchemaDescriptor>> lookup )
    {
        Set<SchemaDescriptor> descriptors = lookup.get( key );
        if ( descriptors == null )
        {
            descriptors = new HashSet<>();
            lookup.put( key, descriptors );
        }
        descriptors.add( schema );
    }

    private void removeFromLookup( int[] keys, SchemaDescriptor schema, MutableIntObjectMap<Set<SchemaDescriptor>> lookup )
    {
        for ( int key : keys )
        {
            Set<SchemaDescriptor> descriptors = lookup.get( key );
            descriptors.remove( schema );
            if ( descriptors.isEmpty() )
            {
                lookup.remove( key );
            }
        }
    }

    private static Map<SchemaDescriptor, IndexProxy> indexesByDescriptor( LongObjectMap<IndexProxy> indexesById )
    {
        return indexesById.toMap( indexProxy -> indexProxy.getDescriptor().schema(), Functions.identity() );
    }

    private static MutableObjectLongMap<SchemaDescriptor> indexIdsByDescriptor( LongObjectMap<IndexProxy> indexesById )
    {
        final MutableObjectLongMap<SchemaDescriptor> map = new ObjectLongHashMap<>( indexesById.size() );
        indexesById.forEachKeyValue( ( id, indexProxy ) -> map.put( indexProxy.getDescriptor().schema(), id ) );
        return map;
    }

    /**
     * Get descriptors affected by changed properties. Implementation checks whether doing
     * the lookup using the unchanged labels or the changed properties given the smallest final
     * set of indexes, and chooses the best way.
     *
     * @param changedEntityTokens set of entity token ids that are changed
     * @param unchangedEntityTokens set of entity token ids that are unchanged
     * @param properties set of properties that have changed
     * @param descriptorsByEntityToken map from entity token id to descriptors
     * @param descriptorsByProperty map from property ids to descriptors
     * @return set of SchemaDescriptors describing the potentially affected indexes
     */
    private Set<SchemaDescriptor> getRelatedDescriptors( long[] changedEntityTokens, long[] unchangedEntityTokens, IntSet properties,
            IntObjectMap<Set<SchemaDescriptor>> descriptorsByEntityToken, IntObjectMap<Set<SchemaDescriptor>> descriptorsByProperty )
    {
        // Grab all indexes relevant to a changed property.
        Set<SchemaDescriptor> indexesByProperties = extractIndexesByProperties( properties, descriptorsByProperty );

        // Make sure that that index really is relevant by intersecting it with indexes relevant for unchanged entity tokens.
        Set<SchemaDescriptor> indexesByUnchangedEntityTokens = extractIndexesByEntityTokens( unchangedEntityTokens, descriptorsByEntityToken );
        indexesByProperties.retainAll( indexesByUnchangedEntityTokens );

        // Add the indexes relevant for the changed entity tokens.
        Set<SchemaDescriptor> descriptors = extractIndexesByEntityTokens( changedEntityTokens, descriptorsByEntityToken );
        descriptors.addAll( indexesByProperties );
        return descriptors;
    }

    private Set<SchemaDescriptor> extractIndexesByEntityTokens( long[] entityTokenIds, IntObjectMap<Set<SchemaDescriptor>> descriptors )
    {
        Set<SchemaDescriptor> set = new HashSet<>();
        for ( long label : entityTokenIds )
        {
            Set<SchemaDescriptor> forLabel = descriptors.get( (int) label );
            if ( forLabel != null )
            {
                set.addAll( forLabel );
            }
        }
        return set;
    }

    private Set<SchemaDescriptor> extractIndexesByProperties( IntSet properties, IntObjectMap<Set<SchemaDescriptor>> descriptorsByProperty )
    {
        Set<SchemaDescriptor> set = new HashSet<>();
        for ( IntIterator iterator = properties.intIterator(); iterator.hasNext(); )
        {
            Set<SchemaDescriptor> forProperty = descriptorsByProperty.get( iterator.next() );
            if ( forProperty != null )
            {
                set.addAll( forProperty );
            }
        }
        return set;
    }
}
