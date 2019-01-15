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
package org.neo4j.internal.recordstorage;

import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.common.EntityType;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorPredicates;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.storageengine.api.SchemaRule;
import org.neo4j.storageengine.api.StorageIndexReference;
import org.neo4j.storageengine.api.schema.ConstraintDescriptor;
import org.neo4j.storageengine.api.schema.SchemaDescriptor;
import org.neo4j.storageengine.api.schema.SchemaDescriptorSupplier;

import static java.util.Collections.emptyIterator;

/**
 * A cache of {@link SchemaRule schema rules} as well as enforcement of schema consistency.
 * Will always reflect the committed state of the schema store.
 */
public class SchemaCache
{
    private final Lock cacheUpdateLock = new StampedLock().asWriteLock();
    private final SchemaRuleAccess schemaRuleAccess;
    private volatile SchemaCacheState schemaCacheState;

    public SchemaCache( ConstraintSemantics constraintSemantics, SchemaRuleAccess schemaRuleAccess )
    {
        this.schemaRuleAccess = schemaRuleAccess;
        this.schemaCacheState = new SchemaCacheState( constraintSemantics, Collections.emptyList() );
    }

    public Iterable<StorageIndexReference> indexDescriptors()
    {
        return schemaCacheState.indexDescriptors();
    }

    Iterable<ConstraintRule> constraintRules()
    {
        return schemaCacheState.constraintRules();
    }

    boolean hasConstraintRule( Long constraintRuleId )
    {
        return schemaCacheState.hasConstraintRule( constraintRuleId );
    }

    public boolean hasConstraintRule( ConstraintDescriptor descriptor )
    {
        return schemaCacheState.hasConstraintRule( descriptor );
    }

    public boolean hasIndex( SchemaDescriptor descriptor )
    {
        return schemaCacheState.hasIndex( descriptor );
    }

    public Iterator<ConstraintDescriptor> constraints()
    {
        return schemaCacheState.constraints();
    }

    Iterator<ConstraintDescriptor> constraintsForLabel( final int label )
    {
        return Iterators.filter( SchemaDescriptorPredicates.hasLabel( label ), constraints() );
    }

    Iterator<ConstraintDescriptor> constraintsForRelationshipType( final int relTypeId )
    {
        return Iterators.filter( SchemaDescriptorPredicates.hasRelType( relTypeId ), constraints() );
    }

    Iterator<ConstraintDescriptor> constraintsForSchema( SchemaDescriptor descriptor )
    {
        return Iterators.filter( SchemaDescriptor.equalTo( descriptor ), constraints() );
    }

    <P, T> T getOrCreateDependantState( Class<T> type, Function<P,T> factory, P parameter )
    {
        return schemaCacheState.getOrCreateDependantState( type, factory, parameter );
    }

    public void loadAllRules()
    {
        cacheUpdateLock.lock();
        try
        {
            ConstraintSemantics constraintSemantics = schemaCacheState.constraintSemantics;
            this.schemaCacheState = new SchemaCacheState( constraintSemantics, schemaRuleAccess.getAll() );
        }
        finally
        {
            cacheUpdateLock.unlock();
        }
    }

    public void addSchemaRule( SchemaRule rule )
    {
        cacheUpdateLock.lock();
        try
        {
            SchemaCacheState updatedSchemaState = new SchemaCacheState( schemaCacheState );
            updatedSchemaState.addSchemaRule( rule );
            this.schemaCacheState = updatedSchemaState;
        }
        finally
        {
            cacheUpdateLock.unlock();
        }
    }

    void removeSchemaRule( long id )
    {
        cacheUpdateLock.lock();
        try
        {
            SchemaCacheState updatedSchemaState = new SchemaCacheState( schemaCacheState );
            updatedSchemaState.removeSchemaRule( id );
            this.schemaCacheState = updatedSchemaState;
        }
        finally
        {
            cacheUpdateLock.unlock();
        }
    }

    public StorageIndexReference indexDescriptor( SchemaDescriptor descriptor )
    {
        return schemaCacheState.indexDescriptor( descriptor );
    }

    Iterator<StorageIndexReference> indexDescriptorsForLabel( int labelId )
    {
        return schemaCacheState.indexDescriptorsForLabel( labelId );
    }

    Iterator<StorageIndexReference> indexesByNodeProperty( int propertyId )
    {
        return schemaCacheState.indexesByNodeProperty( propertyId );
    }

    StorageIndexReference indexDescriptorForName( String name )
    {
        return schemaCacheState.indexDescriptorByName( name );
    }

    public <INDEX_KEY extends SchemaDescriptorSupplier> Set<INDEX_KEY> getIndexesRelatedTo(
            long[] changedEntityTokens, long[] unchangedEntityTokens, IntSet properties, EntityType entityType,
            Function<StorageIndexReference,INDEX_KEY> converter )
    {
        return schemaCacheState.getIndexesRelatedTo( changedEntityTokens, unchangedEntityTokens, properties, entityType, converter );
    }

    private static class SchemaCacheState
    {
        private final ConstraintSemantics constraintSemantics;
        private final Set<ConstraintDescriptor> constraints;
        private final MutableLongObjectMap<StorageIndexReference> indexDescriptorById;
        private final MutableLongObjectMap<ConstraintRule> constraintRuleById;

        private final Map<SchemaDescriptor,StorageIndexReference> indexDescriptors;
        private final EntityDescriptors indexDescriptorsByNode;
        private final EntityDescriptors indexDescriptorsByRelationship;
        private final Map<String,StorageIndexReference> indexDescriptorsByName;

        private final Map<Class<?>,Object> dependantState;

        SchemaCacheState( ConstraintSemantics constraintSemantics, Iterable<SchemaRule> rules )
        {
            this.constraintSemantics = constraintSemantics;
            this.constraints = new HashSet<>();
            this.indexDescriptorById = new LongObjectHashMap<>();
            this.constraintRuleById = new LongObjectHashMap<>();

            this.indexDescriptors = new HashMap<>();
            this.indexDescriptorsByNode = new EntityDescriptors();
            this.indexDescriptorsByRelationship = new EntityDescriptors();
            this.indexDescriptorsByName = new HashMap<>();
            this.dependantState = new ConcurrentHashMap<>();
            load( rules );
        }

        SchemaCacheState( SchemaCacheState schemaCacheState )
        {
            this.constraintSemantics = schemaCacheState.constraintSemantics;
            this.indexDescriptorById = LongObjectHashMap.newMap( schemaCacheState.indexDescriptorById );
            this.constraintRuleById = LongObjectHashMap.newMap( schemaCacheState.constraintRuleById );
            this.constraints = new HashSet<>( schemaCacheState.constraints );

            this.indexDescriptors = new HashMap<>( schemaCacheState.indexDescriptors );
            this.indexDescriptorsByNode = new EntityDescriptors( schemaCacheState.indexDescriptorsByNode );
            this.indexDescriptorsByRelationship = new EntityDescriptors( schemaCacheState.indexDescriptorsByRelationship );
            this.indexDescriptorsByName = new HashMap<>( schemaCacheState.indexDescriptorsByName );
            this.dependantState = new ConcurrentHashMap<>();
        }

        private void load( Iterable<SchemaRule> schemaRuleIterator )
        {
            for ( SchemaRule schemaRule : schemaRuleIterator )
            {
                addSchemaRule( schemaRule );
            }
        }

        Iterable<StorageIndexReference> indexDescriptors()
        {
            return indexDescriptorById.values();
        }

        Iterable<ConstraintRule> constraintRules()
        {
            return constraintRuleById.values();
        }

        boolean hasConstraintRule( Long constraintRuleId )
        {
            return constraintRuleId != null && constraintRuleById.containsKey( constraintRuleId );
        }

        boolean hasConstraintRule( ConstraintDescriptor descriptor )
        {
            return constraints.contains( descriptor );
        }

        boolean hasIndex( SchemaDescriptor descriptor )
        {
            return indexDescriptors.containsKey( descriptor );
        }

        Iterator<ConstraintDescriptor> constraints()
        {
            return constraints.iterator();
        }

        StorageIndexReference indexDescriptor( SchemaDescriptor descriptor )
        {
            return indexDescriptors.get( descriptor );
        }

        StorageIndexReference indexDescriptorByName( String name )
        {
            return indexDescriptorsByName.get( name );
        }

        Iterator<StorageIndexReference> indexesByNodeProperty( int propertyId )
        {
            Set<StorageIndexReference> indexes = indexDescriptorsByNode.byPropertyKey.get( propertyId );
            return (indexes == null) ? emptyIterator() : indexes.iterator();
        }

        Iterator<StorageIndexReference> indexDescriptorsForLabel( int labelId )
        {
            Set<StorageIndexReference> forLabel = indexDescriptorsByNode.byEntity.get( labelId );
            return forLabel == null ? emptyIterator() : forLabel.iterator();
        }

        <INDEX_KEY extends SchemaDescriptorSupplier> Set<INDEX_KEY> getIndexesRelatedTo( long[] changedEntityTokens, long[] unchangedEntityTokens,
                IntSet properties, EntityType entityType, Function<StorageIndexReference,INDEX_KEY> converter )
        {
            EntityDescriptors entityDescriptors = descriptorsByEntityType( entityType );

            // Grab all indexes relevant to a changed property.
            Set<StorageIndexReference> indexesByProperties = extractIndexesByProperties( properties, entityDescriptors.byPropertyKey );

            // Make sure that that index really is relevant by intersecting it with indexes relevant for unchanged entity tokens.
            Set<StorageIndexReference> indexesByUnchangedEntityTokens = new HashSet<>();
            visitIndexesByEntityTokens( unchangedEntityTokens, entityDescriptors.byEntity, indexesByUnchangedEntityTokens::add );
            indexesByProperties.retainAll( indexesByUnchangedEntityTokens );

            // Add the indexes relevant for the changed entity tokens.
            Set<INDEX_KEY> descriptors = new HashSet<>();
            visitIndexesByEntityTokens( changedEntityTokens, entityDescriptors.byEntity, index -> descriptors.add( converter.apply( index ) ) );
            indexesByProperties.forEach( index -> descriptors.add( converter.apply( index ) ) );
            return descriptors;
        }

        private EntityDescriptors descriptorsByEntityType( EntityType entityType )
        {
            switch ( entityType )
            {
            case NODE:
                return indexDescriptorsByNode;
            case RELATIONSHIP:
                return indexDescriptorsByRelationship;
            default:
                throw new IllegalArgumentException( entityType.name() );
            }
        }

        private void visitIndexesByEntityTokens( long[] entityTokenIds, IntObjectMap<Set<StorageIndexReference>> descriptors,
                Consumer<StorageIndexReference> visitor )
        {
            for ( long label : entityTokenIds )
            {
                Set<StorageIndexReference> forLabel = descriptors.get( (int) label );
                if ( forLabel != null )
                {
                    for ( StorageIndexReference match : forLabel )
                    {
                        visitor.accept( match );
                    }
                }
            }
        }

        private Set<StorageIndexReference> extractIndexesByProperties( IntSet properties, IntObjectMap<Set<StorageIndexReference>> descriptorsByProperty )
        {
            Set<StorageIndexReference> set = new HashSet<>();
            for ( IntIterator iterator = properties.intIterator(); iterator.hasNext(); )
            {
                Set<StorageIndexReference> forProperty = descriptorsByProperty.get( iterator.next() );
                if ( forProperty != null )
                {
                    set.addAll( forProperty );
                }
            }
            return set;
        }

        <P, T> T getOrCreateDependantState( Class<T> type, Function<P,T> factory, P parameter )
        {
            return type.cast( dependantState.computeIfAbsent( type, key -> factory.apply( parameter ) ) );
        }

        void addSchemaRule( SchemaRule rule )
        {
            if ( rule instanceof ConstraintRule )
            {
                ConstraintRule constraintRule = (ConstraintRule) rule;
                constraintRuleById.put( constraintRule.getId(), constraintRule );
                constraints.add( constraintSemantics.readConstraint( constraintRule ) );
            }
            else if ( rule instanceof StorageIndexReference )
            {
                StorageIndexReference index = (StorageIndexReference) rule;
                indexDescriptorById.put( index.indexReference(), index );
                SchemaDescriptor schemaDescriptor = index.schema();
                indexDescriptors.put( schemaDescriptor, index );
                indexDescriptorsByName.put( rule.getName(), index );

                // Per entity type
                EntityDescriptors entityDescriptors = descriptorsByEntityType( schemaDescriptor.entityType() );
                for ( int entityToken : schemaDescriptor.getEntityTokenIds() )
                {
                    entityDescriptors.byEntity.getIfAbsentPut( entityToken, HashSet::new ).add( index );
                }
                for ( int propertyKeyToken : schemaDescriptor.getPropertyIds() )
                {
                    entityDescriptors.byPropertyKey.getIfAbsentPut( propertyKeyToken, HashSet::new ).add( index );
                }
            }
        }

        void removeSchemaRule( long id )
        {
            if ( constraintRuleById.containsKey( id ) )
            {
                ConstraintRule rule = constraintRuleById.remove( id );
                constraints.remove( rule.getConstraintDescriptor() );
            }
            else if ( indexDescriptorById.containsKey( id ) )
            {
                StorageIndexReference index = indexDescriptorById.remove( id );
                SchemaDescriptor schema = index.schema();
                indexDescriptors.remove( schema );
                indexDescriptorsByName.remove( index.name(), index );

                EntityDescriptors entityDescriptors = descriptorsByEntityType( schema.entityType() );
                for ( int entityTokenId : schema.getEntityTokenIds() )
                {
                    Set<StorageIndexReference> forLabel = entityDescriptors.byEntity.get( entityTokenId );
                    forLabel.remove( index );
                    if ( forLabel.isEmpty() )
                    {
                        entityDescriptors.byEntity.remove( entityTokenId );
                    }
                }

                for ( int propertyId : index.schema().getPropertyIds() )
                {
                    Set<StorageIndexReference> forProperty = entityDescriptors.byPropertyKey.get( propertyId );
                    forProperty.remove( index );
                    if ( forProperty.isEmpty() )
                    {
                        entityDescriptors.byPropertyKey.remove( propertyId );
                    }
                }
            }
        }

        private static class EntityDescriptors
        {
            private final MutableIntObjectMap<Set<StorageIndexReference>> byEntity;
            private final MutableIntObjectMap<Set<StorageIndexReference>> byPropertyKey;

            EntityDescriptors( EntityDescriptors copyFrom )
            {
                byEntity = new IntObjectHashMap<>( copyFrom.byEntity.size() );
                byPropertyKey = new IntObjectHashMap<>( copyFrom.byPropertyKey.size() );
                copyFrom.byEntity.forEachKeyValue( ( k, v ) -> byEntity.put( k, new HashSet<>( v ) ) );
                copyFrom.byPropertyKey.forEachKeyValue( ( k, v ) -> byPropertyKey.put( k, new HashSet<>( v ) ) );
            }

            EntityDescriptors()
            {
                byEntity = new IntObjectHashMap<>();
                byPropertyKey = new IntObjectHashMap<>();
            }
        }
    }
}
