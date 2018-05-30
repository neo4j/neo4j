/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.storageengine.api.EntityType;

import static org.neo4j.internal.kernel.api.schema.SchemaDescriptor.ANY_ENTITY_TOKEN;

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
    private final Set<SchemaDescriptor> descriptorsForAllLabels;
    private final Set<SchemaDescriptor> descriptorsForAllReltypes;

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
        this.descriptorsForAllLabels = new HashSet<>();
        this.descriptorsForAllReltypes = new HashSet<>();
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
            if ( Arrays.equals(schema.getEntityTokenIds(), ANY_ENTITY_TOKEN ) )
            {
                descriptorsForAllLabels.remove( schema );
            }
            for ( int entityTokenId : schema.getEntityTokenIds() )
            {
                removeFromLookup( entityTokenId, schema, descriptorsByLabel );
            }
            for ( int propertyId : schema.getPropertyIds() )
            {
                removeFromLookup( propertyId, schema, nodeDescriptorsByProperty );
            }
        }
        else if ( schema.entityType() == EntityType.RELATIONSHIP )
        {
            if ( Arrays.equals(schema.getEntityTokenIds(), ANY_ENTITY_TOKEN ) )
            {
                descriptorsForAllReltypes.remove( schema );
            }
            for ( int entityTokenId : schema.getEntityTokenIds() )
            {
                removeFromLookup( entityTokenId, schema, descriptorsByReltype );
            }
            for ( int propertyId : schema.getPropertyIds() )
            {
                removeFromLookup( propertyId, schema, relationshipDescriptorsByProperty );
            }
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
        Function<long[],Set<SchemaDescriptor>> indexesByEntityIds;
        BiFunction<long[],IntSet,Set<SchemaDescriptor>> indexesByProperties;
        if ( entityType == EntityType.NODE )
        {
            indexesByEntityIds = labels -> extractIndexesByEntityTokens( labels, descriptorsByLabel, descriptorsForAllLabels );
            indexesByProperties = ( unchangedLabels, changedProperties ) -> getDescriptorsByProperties( unchangedLabels, changedProperties,
                    descriptorsByLabel, descriptorsForAllLabels, nodeDescriptorsByProperty );
        }
        else
        {
            indexesByEntityIds = reltypes -> extractIndexesByEntityTokens( reltypes, descriptorsByReltype, descriptorsForAllReltypes );
            indexesByProperties = ( unchangedLabels, changedProperties ) -> getDescriptorsByProperties( unchangedLabels, changedProperties,
                    descriptorsByReltype, descriptorsForAllReltypes, relationshipDescriptorsByProperty );
        }

        if ( properties.isEmpty() )
        {
            Set<SchemaDescriptor> descriptors = indexesByEntityIds.apply( changedEntityTokens );
            return descriptors == null ? Collections.emptySet() : descriptors;
        }
        if ( changedEntityTokens.length == 0 )
        {
            return indexesByProperties.apply( unchangedEntityTokens, properties );
        }
        Set<SchemaDescriptor> descriptors = indexesByEntityIds.apply( changedEntityTokens );
        descriptors.addAll( indexesByProperties.apply( unchangedEntityTokens, properties ) );

        return descriptors;
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
            if ( Arrays.equals(schema.getEntityTokenIds(), ANY_ENTITY_TOKEN ) )
            {
                descriptorsForAllLabels.add( schema );
            }
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
            if ( Arrays.equals(schema.getEntityTokenIds(), ANY_ENTITY_TOKEN ) )
            {
                descriptorsForAllReltypes.add( schema );
            }
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

    private void removeFromLookup(
            int key,
            SchemaDescriptor schema,
            MutableIntObjectMap<Set<SchemaDescriptor>> lookup )
    {
        Set<SchemaDescriptor> descriptors = lookup.get( key );
        descriptors.remove( schema );
        if ( descriptors.isEmpty() )
        {
            lookup.remove( key );
        }
    }

    private static Map<SchemaDescriptor, IndexProxy> indexesByDescriptor( LongObjectMap<IndexProxy> indexesById )
    {
        return indexesById.toMap( indexProxy -> indexProxy.getDescriptor().schema(), Functions.identity() );
    }

    private static MutableObjectLongMap<SchemaDescriptor> indexIdsByDescriptor( LongObjectMap<IndexProxy> indexesById )
    {
        final MutableObjectLongMap<SchemaDescriptor> map = new ObjectLongHashMap<>( indexesById.size() );
        indexesById.forEachKeyValue( ( id, indexProxy ) ->
        {
            map.put( indexProxy.getDescriptor().schema(), id );

        } );
        return map;
    }

    /**
     * Get descriptors affected by changed properties. Implementation checks whether doing
     * the lookup using the unchanged labels or the changed properties given the smallest final
     * set of indexes, and chooses the best way.
     *
     * @param unchangedEntityTokens set of labels that are unchanged
     * @param properties set of properties that have changed
     * @param descriptorsByEntityToken map from entity token id to descriptors
     * @param descriptorsForAllEntityTokens set of descriptors for all entity tokens
     * @param descriptorsByProperty map from property ids to descriptors
     * @return set of SchemaDescriptors describing the potentially affected indexes
     */
    private Set<SchemaDescriptor> getDescriptorsByProperties( long[] unchangedEntityTokens, IntSet properties,
            IntObjectMap<Set<SchemaDescriptor>> descriptorsByEntityToken, Set<SchemaDescriptor> descriptorsForAllEntityTokens,
            IntObjectMap<Set<SchemaDescriptor>> descriptorsByProperty )
    {
        int nIndexesForEntityTokens = countIndexesByEntityTokens( unchangedEntityTokens, descriptorsByEntityToken, descriptorsForAllEntityTokens );
        int nIndexesForProperties = countIndexesByProperties( properties, descriptorsByProperty );

        // Our lowest bound is zero applicable indexes, i.e. no indexes are relevant.
        if ( nIndexesForEntityTokens == 0 || nIndexesForProperties == 0 )
        {
            return Collections.emptySet();
        }
        // Even if we don't have any token ids, we still need to return the anytoken indexes for the given properties.
        if ( unchangedEntityTokens.length == 0 )
        {
            Set<SchemaDescriptor> descriptors = extractIndexesByProperties( properties, descriptorsByProperty );
            descriptors.retainAll( descriptorsForAllEntityTokens );
            return descriptors;
        }
        // Grab indexes in the fashion that results in the smallest set of indexes.
        if ( nIndexesForEntityTokens < nIndexesForProperties )
        {
            return extractIndexesByEntityTokens( unchangedEntityTokens, descriptorsByEntityToken, descriptorsForAllEntityTokens );
        }
        else
        {
            return extractIndexesByProperties( properties, descriptorsByProperty );
        }
    }

    private Set<SchemaDescriptor> extractIndexesByEntityTokens( long[] entityTokenIds, IntObjectMap<Set<SchemaDescriptor>> descriptors,
            Set<SchemaDescriptor> descriptorsForAllEntityTokens )
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
        if ( entityTokenIds.length != 0 )
        {
            set.addAll( descriptorsForAllEntityTokens );
        }
        return set;
    }

    private int countIndexesByEntityTokens( long[] entityTokenIds, IntObjectMap<Set<SchemaDescriptor>> descriptors,
            Set<SchemaDescriptor> descriptorsForAllEntityTokenIds )
    {
        int count = 0;
        for ( long entityToken : entityTokenIds )
        {
            Set<SchemaDescriptor> forEntityToken = descriptors.get( (int) entityToken );
            if ( forEntityToken != null )
            {
                count += forEntityToken.size();
            }
        }
        count += descriptorsForAllEntityTokenIds.size();
        return count;
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

    private int countIndexesByProperties( IntSet properties, IntObjectMap<Set<SchemaDescriptor>> descriptorsByProperty )
    {
        int count = 0;

        for ( IntIterator iterator = properties.intIterator(); iterator.hasNext(); )
        {
            Set<SchemaDescriptor> forProperty = descriptorsByProperty.get( iterator.next() );
            if ( forProperty != null )
            {
                count += forProperty.size();
            }
        }
        return count;
    }
}
