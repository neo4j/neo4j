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

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

import org.neo4j.common.EntityType;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorLookupSet;
import org.neo4j.internal.schema.SchemaDescriptorPredicates;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;

/**
 * A cache of {@link SchemaRule schema rules} as well as enforcement of schema consistency.
 * Will always reflect the committed state of the schema store.
 */
public class SchemaCache
{
    private final Lock cacheUpdateLock;
    private volatile SchemaCacheState schemaCacheState;

    public SchemaCache( ConstraintRuleAccessor constraintSemantics, IndexConfigCompleter indexConfigCompleter )
    {
        this.cacheUpdateLock = new StampedLock().asWriteLock();
        this.schemaCacheState = new SchemaCacheState( constraintSemantics, indexConfigCompleter, Collections.emptyList() );
    }

    /**
     * Snapshot constructor. This is only used by the {@link #snapshot()} method.
     */
    private SchemaCache( SchemaCacheState schemaCacheState )
    {
        this.cacheUpdateLock = new InaccessibleLock( "Schema cache snapshots are read-only." );
        this.schemaCacheState = schemaCacheState;
    }

    public Iterable<IndexDescriptor> indexDescriptors()
    {
        return schemaCacheState.indexDescriptors();
    }

    Iterable<ConstraintDescriptor> constraintRules()
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

    public void load( Iterable<SchemaRule> rules )
    {
        cacheUpdateLock.lock();
        try
        {
            ConstraintRuleAccessor constraintSemantics = schemaCacheState.constraintSemantics;
            IndexConfigCompleter indexConfigCompleter = schemaCacheState.indexConfigCompleter;
            this.schemaCacheState = new SchemaCacheState( constraintSemantics, indexConfigCompleter, rules );
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

    public IndexDescriptor indexDescriptor( SchemaDescriptor descriptor )
    {
        return schemaCacheState.indexDescriptor( descriptor );
    }

    Iterator<IndexDescriptor> indexDescriptorsForLabel( int labelId )
    {
        return schemaCacheState.indexDescriptorsForLabel( labelId );
    }

    Iterator<IndexDescriptor> indexDescriptorsForRelationshipType( int relationshipType )
    {
        return schemaCacheState.indexDescriptorsForRelationshipType( relationshipType );
    }

    IndexDescriptor indexDescriptorForName( String name )
    {
        return schemaCacheState.indexDescriptorByName( name );
    }

    ConstraintDescriptor constraintForName( String name )
    {
        return schemaCacheState.constraintByName( name );
    }

    public Set<SchemaDescriptor> getIndexesRelatedTo(
            long[] changedEntityTokens, long[] unchangedEntityTokens, int[] properties,
            boolean propertyListIsComplete, EntityType entityType )
    {
        return schemaCacheState.getIndexesRelatedTo( entityType, changedEntityTokens, unchangedEntityTokens, properties, propertyListIsComplete );
    }

    Collection<IndexBackedConstraintDescriptor> getUniquenessConstraintsRelatedTo( long[] changedLabels, long[] unchangedLabels, int[] properties,
            boolean propertyListIsComplete, EntityType entityType )
    {
        return schemaCacheState.getUniquenessConstraintsRelatedTo( entityType, changedLabels, unchangedLabels, properties, propertyListIsComplete );
    }

    boolean hasRelatedSchema( long[] labels, int propertyKey, EntityType entityType )
    {
        return schemaCacheState.hasRelatedSchema( labels, propertyKey, entityType );
    }

    boolean hasRelatedSchema( int label, EntityType entityType )
    {
        return schemaCacheState.hasRelatedSchema( label, entityType );
    }

    public SchemaCache snapshot()
    {
        return new SchemaCache( schemaCacheState );
    }

    private static class SchemaCacheState
    {
        private final ConstraintRuleAccessor constraintSemantics;
        private final IndexConfigCompleter indexConfigCompleter;
        private final Set<ConstraintDescriptor> constraints;
        private final MutableLongObjectMap<IndexDescriptor> indexDescriptorById;
        private final MutableLongObjectMap<ConstraintDescriptor> constraintRuleById;

        private final Map<SchemaDescriptor,IndexDescriptor> indexDescriptors;
        private final SchemaDescriptorLookupSet<SchemaDescriptor> indexDescriptorsByNode;
        private final SchemaDescriptorLookupSet<SchemaDescriptor> indexDescriptorsByRelationship;
        private final SchemaDescriptorLookupSet<IndexBackedConstraintDescriptor> uniquenessConstraintsByNode;
        private final SchemaDescriptorLookupSet<IndexBackedConstraintDescriptor> uniquenessConstraintsByRelationship;
        private final Map<String,IndexDescriptor> indexDescriptorsByName;
        private final Map<String,ConstraintDescriptor> constrainsByName;

        private final Map<Class<?>,Object> dependantState;

        SchemaCacheState( ConstraintRuleAccessor constraintSemantics, IndexConfigCompleter indexConfigCompleter, Iterable<SchemaRule> rules )
        {
            this.constraintSemantics = constraintSemantics;
            this.indexConfigCompleter = indexConfigCompleter;
            this.constraints = new HashSet<>();
            this.indexDescriptorById = new LongObjectHashMap<>();
            this.constraintRuleById = new LongObjectHashMap<>();

            this.indexDescriptors = new HashMap<>();
            this.indexDescriptorsByNode = new SchemaDescriptorLookupSet<>();
            this.indexDescriptorsByRelationship = new SchemaDescriptorLookupSet<>();
            this.uniquenessConstraintsByNode = new SchemaDescriptorLookupSet<>();
            this.uniquenessConstraintsByRelationship = new SchemaDescriptorLookupSet<>();
            this.indexDescriptorsByName = new HashMap<>();
            this.constrainsByName = new HashMap<>();
            this.dependantState = new ConcurrentHashMap<>();
            load( rules );
        }

        SchemaCacheState( SchemaCacheState schemaCacheState )
        {
            this.constraintSemantics = schemaCacheState.constraintSemantics;
            this.indexConfigCompleter = schemaCacheState.indexConfigCompleter;
            this.indexDescriptorById = LongObjectHashMap.newMap( schemaCacheState.indexDescriptorById );
            this.constraintRuleById = LongObjectHashMap.newMap( schemaCacheState.constraintRuleById );
            this.constraints = new HashSet<>( schemaCacheState.constraints );

            this.indexDescriptors = new HashMap<>( schemaCacheState.indexDescriptors );
            this.indexDescriptorsByNode = new SchemaDescriptorLookupSet<>();
            this.indexDescriptorsByRelationship = new SchemaDescriptorLookupSet<>();
            this.uniquenessConstraintsByNode = new SchemaDescriptorLookupSet<>();
            this.uniquenessConstraintsByRelationship = new SchemaDescriptorLookupSet<>();
            // Now fill the node/relationship sets
            this.indexDescriptorById.forEachValue( index -> selectIndexSetByEntityType( index.schema().entityType() ).add( index.schema() ) );
            this.constraintRuleById.forEachValue( this::cacheUniquenessConstraint );
            this.indexDescriptorsByName = new HashMap<>( schemaCacheState.indexDescriptorsByName );
            this.constrainsByName = new HashMap<>( schemaCacheState.constrainsByName );
            this.dependantState = new ConcurrentHashMap<>();
        }

        private void cacheUniquenessConstraint( ConstraintDescriptor constraint )
        {
            if ( constraint.enforcesUniqueness() )
            {
                selectUniquenessConstraintSetByEntityType( constraint.schema().entityType() ).add( constraint.asIndexBackedConstraint() );
            }
        }

        private void load( Iterable<SchemaRule> schemaRuleIterator )
        {
            for ( SchemaRule schemaRule : schemaRuleIterator )
            {
                addSchemaRule( schemaRule );
            }
        }

        Iterable<IndexDescriptor> indexDescriptors()
        {
            return indexDescriptorById.values();
        }

        Iterable<ConstraintDescriptor> constraintRules()
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

        IndexDescriptor indexDescriptor( SchemaDescriptor descriptor )
        {
            return indexDescriptors.get( descriptor );
        }

        IndexDescriptor indexDescriptorByName( String name )
        {
            return indexDescriptorsByName.get( name );
        }

        ConstraintDescriptor constraintByName( String name )
        {
            return constrainsByName.get( name );
        }

        Iterator<IndexDescriptor> indexDescriptorsForLabel( int labelId )
        {
            return Iterators.map( indexDescriptors::get,
                    getSchemaRelatedTo( indexDescriptorsByNode, new long[]{labelId}, EMPTY_LONG_ARRAY, EMPTY_INT_ARRAY, false ).iterator() );
        }

        Iterator<IndexDescriptor> indexDescriptorsForRelationshipType( int relationshipType )
        {
            return Iterators.map( indexDescriptors::get,
                    getSchemaRelatedTo( indexDescriptorsByRelationship, new long[]{relationshipType}, EMPTY_LONG_ARRAY, EMPTY_INT_ARRAY, false ).iterator() );
        }

        Set<SchemaDescriptor> getIndexesRelatedTo( EntityType entityType, long[] changedEntityTokens,
                long[] unchangedEntityTokens, int[] properties, boolean propertyListIsComplete )
        {
            return getSchemaRelatedTo( selectIndexSetByEntityType( entityType ), changedEntityTokens, unchangedEntityTokens, properties,
                    propertyListIsComplete );
        }

        Set<IndexBackedConstraintDescriptor> getUniquenessConstraintsRelatedTo( EntityType entityType, long[] changedEntityTokens,
                long[] unchangedEntityTokens, int[] properties, boolean propertyListIsComplete )
        {
            return getSchemaRelatedTo( selectUniquenessConstraintSetByEntityType( entityType ), changedEntityTokens, unchangedEntityTokens, properties,
                    propertyListIsComplete );
        }

        <T extends SchemaDescriptorSupplier> Set<T> getSchemaRelatedTo( SchemaDescriptorLookupSet<T> set, long[] changedEntityTokens,
                long[] unchangedEntityTokens, int[] properties, boolean propertyListIsComplete )
        {
            if ( set.isEmpty() )
            {
                return Collections.emptySet();
            }

            Set<T> descriptors = new HashSet<>();
            if ( propertyListIsComplete )
            {
                set.matchingDescriptorsForCompleteListOfProperties( descriptors, changedEntityTokens, properties );
            }
            else
            {
                // At the time of writing this the commit process won't load the complete list of property keys for an entity.
                // Because of this the matching cannot be as precise as if the complete list was known.
                // Anyway try to make the best out of it and narrow down the list of potentially related indexes as much as possible.
                if ( properties.length == 0 )
                {
                    // Only labels changed. Since we don't know which properties this entity has let's include all indexes for the changed labels.
                    set.matchingDescriptors( descriptors, changedEntityTokens );
                }
                else if ( changedEntityTokens.length == 0 )
                {
                    // Only properties changed. Since we don't know which other properties this entity has let's include all indexes
                    // for the (unchanged) labels on this entity that has any match on any of the changed properties.
                    set.matchingDescriptorsForPartialListOfProperties( descriptors, unchangedEntityTokens, properties );
                }
                else
                {
                    // Both labels and properties changed.
                    // All indexes for the changed labels must be included.
                    // Also include all indexes for any of the changed or unchanged labels that has any match on any of the changed properties.
                    set.matchingDescriptors( descriptors, changedEntityTokens );
                    set.matchingDescriptorsForPartialListOfProperties( descriptors, unchangedEntityTokens, properties );
                }
            }
            return descriptors;
        }

        boolean hasRelatedSchema( long[] labels, int propertyKey, EntityType entityType )
        {
            return selectIndexSetByEntityType( entityType ).has( labels, propertyKey ) ||
                    selectUniquenessConstraintSetByEntityType( entityType ).has( labels, propertyKey );
        }

        boolean hasRelatedSchema( int label, EntityType entityType )
        {
            return selectIndexSetByEntityType( entityType ).has( label ) ||
                    selectUniquenessConstraintSetByEntityType( entityType ).has( label );
        }

        private SchemaDescriptorLookupSet<SchemaDescriptor> selectIndexSetByEntityType( EntityType entityType )
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

        private SchemaDescriptorLookupSet<IndexBackedConstraintDescriptor> selectUniquenessConstraintSetByEntityType( EntityType entityType )
        {
            switch ( entityType )
            {
            case NODE:
                return uniquenessConstraintsByNode;
            case RELATIONSHIP:
                return uniquenessConstraintsByRelationship;
            default:
                throw new IllegalArgumentException( entityType.name() );
            }
        }

        <P, T> T getOrCreateDependantState( Class<T> type, Function<P,T> factory, P parameter )
        {
            return type.cast( dependantState.computeIfAbsent( type, key -> factory.apply( parameter ) ) );
        }

        void addSchemaRule( SchemaRule rule )
        {
            if ( rule instanceof ConstraintDescriptor )
            {
                ConstraintDescriptor constraint = (ConstraintDescriptor) rule;
                constraintRuleById.put( constraint.getId(), constraint );
                constraints.add( constraintSemantics.readConstraint( constraint ) );
                constrainsByName.put( constraint.getName(), constraint );
                cacheUniquenessConstraint( constraint );
            }
            else if ( rule instanceof IndexDescriptor )
            {
                IndexDescriptor index = indexConfigCompleter.completeConfiguration( (IndexDescriptor) rule );
                indexDescriptorById.put( index.getId(), index );
                SchemaDescriptor schemaDescriptor = index.schema();
                indexDescriptors.put( schemaDescriptor, index );
                indexDescriptorsByName.put( rule.getName(), index );
                selectIndexSetByEntityType( schemaDescriptor.entityType() ).add( schemaDescriptor );
            }
        }

        void removeSchemaRule( long id )
        {
            if ( constraintRuleById.containsKey( id ) )
            {
                ConstraintDescriptor constraint = constraintRuleById.remove( id );
                constraints.remove( constraint );
                constrainsByName.remove( constraint.getName() );
                if ( constraint.enforcesUniqueness() )
                {
                    selectUniquenessConstraintSetByEntityType( constraint.schema().entityType() ).remove( constraint.asIndexBackedConstraint() );
                }
            }
            else if ( indexDescriptorById.containsKey( id ) )
            {
                IndexDescriptor index = indexDescriptorById.remove( id );
                SchemaDescriptor schema = index.schema();
                indexDescriptors.remove( schema );
                indexDescriptorsByName.remove( index.getName(), index );
                selectIndexSetByEntityType( schema.entityType() ).remove( schema );
            }
        }
    }
}
