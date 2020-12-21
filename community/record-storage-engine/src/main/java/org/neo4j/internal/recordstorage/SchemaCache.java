/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

    public Iterable<IndexDescriptor> indexes()
    {
        return schemaCacheState.indexes();
    }

    public Iterable<ConstraintDescriptor> constraints()
    {
        return schemaCacheState.constraints();
    }

    boolean hasConstraintRule( Long constraintRuleId )
    {
        return schemaCacheState.hasConstraintRule( constraintRuleId );
    }

    public boolean hasConstraintRule( ConstraintDescriptor descriptor )
    {
        return schemaCacheState.hasConstraintRule( descriptor );
    }

    public boolean hasIndex( IndexDescriptor index )
    {
        return schemaCacheState.hasIndex( index );
    }

    public boolean hasIndex( SchemaDescriptor descriptor )
    {
        return schemaCacheState.hasIndex( descriptor );
    }

    Iterator<ConstraintDescriptor> constraintsForLabel( final int label )
    {
        return Iterators.filter( SchemaDescriptorPredicates.hasLabel( label ), constraints().iterator() );
    }

    Iterator<ConstraintDescriptor> constraintsForRelationshipType( final int relTypeId )
    {
        return Iterators.filter( SchemaDescriptorPredicates.hasRelType( relTypeId ), constraints().iterator() );
    }

    Iterator<ConstraintDescriptor> constraintsForSchema( SchemaDescriptor descriptor )
    {
        return Iterators.filter( SchemaDescriptor.equalTo( descriptor ), constraints().iterator() );
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

    public IndexDescriptor getIndex( long id )
    {
        return schemaCacheState.getIndex( id );
    }

    public Iterator<IndexDescriptor> indexesForSchema( SchemaDescriptor descriptor )
    {
        return schemaCacheState.indexesForSchema( descriptor );
    }

    Iterator<IndexDescriptor> indexesForLabel( int labelId )
    {
        return schemaCacheState.indexesForLabel( labelId );
    }

    Iterator<IndexDescriptor> indexesForRelationshipType( int relationshipType )
    {
        return schemaCacheState.indexesForRelationshipType( relationshipType );
    }

    IndexDescriptor indexForName( String name )
    {
        return schemaCacheState.indexForName( name );
    }

    ConstraintDescriptor constraintForName( String name )
    {
        return schemaCacheState.constraintForName( name );
    }

    public Set<IndexDescriptor> getIndexesRelatedTo(
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

    boolean hasRelatedSchema( long[] tokens, int propertyKey, EntityType entityType )
    {
        return schemaCacheState.hasRelatedSchema( tokens, propertyKey, entityType );
    }

    boolean hasRelatedSchema( int token, EntityType entityType )
    {
        return schemaCacheState.hasRelatedSchema( token, entityType );
    }

    public SchemaCache snapshot()
    {
        return new SchemaCache( schemaCacheState );
    }

    @SuppressWarnings( "ReplaceNullCheck" )
    private static class SchemaCacheState
    {
        private final ConstraintRuleAccessor constraintSemantics;
        private final IndexConfigCompleter indexConfigCompleter;
        private final MutableLongObjectMap<IndexDescriptor> indexesById;
        private final MutableLongObjectMap<ConstraintDescriptor> constraintsById;
        private final Set<ConstraintDescriptor> constraints;

        private final Map<SchemaDescriptor,IndexDescriptor> indexesBySchema;
        private final SchemaDescriptorLookupSet<IndexDescriptor> indexesByNode;
        private final SchemaDescriptorLookupSet<IndexDescriptor> indexesByRelationship;
        private final SchemaDescriptorLookupSet<IndexBackedConstraintDescriptor> uniquenessConstraintsByNode;
        private final SchemaDescriptorLookupSet<IndexBackedConstraintDescriptor> uniquenessConstraintsByRelationship;
        private final Map<String,IndexDescriptor> indexesByName;
        private final Map<String,ConstraintDescriptor> constrainsByName;

        private final Map<Class<?>,Object> dependantState;
        private final ConcurrentMap<Object,Set<IndexDescriptor>> indexCache; // Cache results of getSchemaRelatedTo queries.
        private final ConcurrentMap<Object,Set<IndexBackedConstraintDescriptor>> constraintCache; // Cache results of getSchemaRelatedTo queries.

        SchemaCacheState( ConstraintRuleAccessor constraintSemantics, IndexConfigCompleter indexConfigCompleter, Iterable<SchemaRule> rules )
        {
            this.constraintSemantics = constraintSemantics;
            this.indexConfigCompleter = indexConfigCompleter;
            this.indexesById = new LongObjectHashMap<>();
            this.constraintsById = new LongObjectHashMap<>();
            this.constraints = new HashSet<>();

            this.indexesBySchema = new HashMap<>();
            this.indexesByNode = new SchemaDescriptorLookupSet<>();
            this.indexesByRelationship = new SchemaDescriptorLookupSet<>();
            this.uniquenessConstraintsByNode = new SchemaDescriptorLookupSet<>();
            this.uniquenessConstraintsByRelationship = new SchemaDescriptorLookupSet<>();
            this.indexesByName = new HashMap<>();
            this.constrainsByName = new HashMap<>();
            this.dependantState = new ConcurrentHashMap<>();
            this.indexCache = new ConcurrentHashMap<>();
            this.constraintCache = new ConcurrentHashMap<>();
            load( rules );
        }

        SchemaCacheState( SchemaCacheState schemaCacheState )
        {
            this.constraintSemantics = schemaCacheState.constraintSemantics;
            this.indexConfigCompleter = schemaCacheState.indexConfigCompleter;
            this.indexesById = LongObjectHashMap.newMap( schemaCacheState.indexesById );
            this.constraintsById = LongObjectHashMap.newMap( schemaCacheState.constraintsById );
            this.constraints = new HashSet<>( schemaCacheState.constraints );

            this.indexesBySchema = new HashMap<>( schemaCacheState.indexesBySchema );
            this.indexesByNode = new SchemaDescriptorLookupSet<>();
            this.indexesByRelationship = new SchemaDescriptorLookupSet<>();
            this.uniquenessConstraintsByNode = new SchemaDescriptorLookupSet<>();
            this.uniquenessConstraintsByRelationship = new SchemaDescriptorLookupSet<>();
            // Now fill the node/relationship sets
            this.indexesById.forEachValue( index -> selectIndexSetByEntityType( index.schema().entityType() ).add( index ) );
            this.constraintsById.forEachValue( this::cacheUniquenessConstraint );
            this.indexesByName = new HashMap<>( schemaCacheState.indexesByName );
            this.constrainsByName = new HashMap<>( schemaCacheState.constrainsByName );
            this.dependantState = new ConcurrentHashMap<>();
            this.indexCache = new ConcurrentHashMap<>();
            this.constraintCache = new ConcurrentHashMap<>();
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

        Iterable<IndexDescriptor> indexes()
        {
            return indexesById.values();
        }

        boolean hasConstraintRule( Long constraintRuleId )
        {
            return constraintRuleId != null && constraintsById.containsKey( constraintRuleId );
        }

        boolean hasConstraintRule( ConstraintDescriptor descriptor )
        {
            return constraints.contains( descriptor );
        }

        boolean hasIndex( IndexDescriptor index )
        {
            return indexesById.containsKey( index.getId() );
        }

        boolean hasIndex( SchemaDescriptor descriptor )
        {
            return indexesBySchema.containsKey( descriptor );
        }

        Iterable<ConstraintDescriptor> constraints()
        {
            return constraints;
        }

        IndexDescriptor getIndex( long id )
        {
            return indexesById.get( id );
        }

        Iterator<IndexDescriptor> indexesForSchema( SchemaDescriptor descriptor )
        {
            IndexDescriptor index = indexesBySchema.get( descriptor );
            return index == null ? Iterators.emptyResourceIterator() : Iterators.iterator( index );
        }

        IndexDescriptor indexForName( String name )
        {
            return indexesByName.get( name );
        }

        ConstraintDescriptor constraintForName( String name )
        {
            return constrainsByName.get( name );
        }

        Iterator<IndexDescriptor> indexesForLabel( int labelId )
        {
            if ( indexesByNode.isEmpty() )
            {
                return Collections.emptyIterator();
            }
            IndexesForLabelKey key = new IndexesForLabelKey( labelId );
            Set<IndexDescriptor> result = indexCache.get( key );
            if ( result != null )
            {
                return result.iterator();
            }
            return indexCache.computeIfAbsent( key,
                    k -> getSchemaRelatedTo( indexesByNode, new long[]{labelId}, EMPTY_LONG_ARRAY, EMPTY_INT_ARRAY, false ) ).iterator();
        }

        Iterator<IndexDescriptor> indexesForRelationshipType( int relationshipType )
        {
            if ( indexesByRelationship.isEmpty() )
            {
                return Collections.emptyIterator();
            }
            IndexesForRelationshipTypeKey key = new IndexesForRelationshipTypeKey( relationshipType );
            Set<IndexDescriptor> result = indexCache.get( key );
            if ( result != null )
            {
                return result.iterator();
            }
            return indexCache.computeIfAbsent( key,
                    k -> getSchemaRelatedTo( indexesByRelationship, new long[]{relationshipType}, EMPTY_LONG_ARRAY, EMPTY_INT_ARRAY, false ) ).iterator();
        }

        Set<IndexDescriptor> getIndexesRelatedTo( EntityType entityType, long[] changedEntityTokens,
                long[] unchangedEntityTokens, int[] properties, boolean propertyListIsComplete )
        {
            SchemaDescriptorLookupSet<IndexDescriptor> set = selectIndexSetByEntityType( entityType );
            if ( set.isEmpty() )
            {
                return Collections.emptySet();
            }
            IndexesRelatedToKey key = new IndexesRelatedToKey( entityType, changedEntityTokens, unchangedEntityTokens, properties, propertyListIsComplete );
            Set<IndexDescriptor> result = indexCache.get( key );
            if ( result != null )
            {
                return result;
            }
            return indexCache.computeIfAbsent( key,
                    k -> getSchemaRelatedTo( set, changedEntityTokens, unchangedEntityTokens, properties, propertyListIsComplete ) );
        }

        Set<IndexBackedConstraintDescriptor> getUniquenessConstraintsRelatedTo( EntityType entityType, long[] changedEntityTokens,
                long[] unchangedEntityTokens, int[] properties, boolean propertyListIsComplete )
        {
            SchemaDescriptorLookupSet<IndexBackedConstraintDescriptor> set = selectUniquenessConstraintSetByEntityType( entityType );
            if ( set.isEmpty() )
            {
                return Collections.emptySet();
            }
            UniqueIndexesRelatedToKey key = new UniqueIndexesRelatedToKey( entityType, changedEntityTokens, unchangedEntityTokens, properties,
                    propertyListIsComplete );
            Set<IndexBackedConstraintDescriptor> result = constraintCache.get( key );
            if ( result != null )
            {
                return result;
            }
            return constraintCache.computeIfAbsent( key,
                    k -> getSchemaRelatedTo( set, changedEntityTokens, unchangedEntityTokens, properties, propertyListIsComplete ) );
        }

        private <T extends SchemaDescriptorSupplier> Set<T> getSchemaRelatedTo( SchemaDescriptorLookupSet<T> set, long[] changedEntityTokens,
                long[] unchangedEntityTokens, int[] properties, boolean propertyListIsComplete )
        {
            Set<T> descriptors = UnifiedSet.newSet();
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

        boolean hasRelatedSchema( long[] tokens, int propertyKey, EntityType entityType )
        {
            return selectIndexSetByEntityType( entityType ).has( tokens, propertyKey ) ||
                    selectUniquenessConstraintSetByEntityType( entityType ).has( tokens, propertyKey );
        }

        boolean hasRelatedSchema( int token, EntityType entityType )
        {
            return selectIndexSetByEntityType( entityType ).has( token ) ||
                    selectUniquenessConstraintSetByEntityType( entityType ).has( token );
        }

        private SchemaDescriptorLookupSet<IndexDescriptor> selectIndexSetByEntityType( EntityType entityType )
        {
            switch ( entityType )
            {
            case NODE:
                return indexesByNode;
            case RELATIONSHIP:
                return indexesByRelationship;
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
                constraint = constraintSemantics.readConstraint( constraint );
                constraintsById.put( constraint.getId(), constraint );
                constrainsByName.put( constraint.getName(), constraint );
                constraints.add( constraint );
                cacheUniquenessConstraint( constraint );
            }
            else if ( rule instanceof IndexDescriptor )
            {
                IndexDescriptor index = indexConfigCompleter.completeConfiguration( (IndexDescriptor) rule );
                indexesById.put( index.getId(), index );
                SchemaDescriptor schemaDescriptor = index.schema();
                indexesBySchema.put( schemaDescriptor, index );
                indexesByName.put( rule.getName(), index );
                selectIndexSetByEntityType( schemaDescriptor.entityType() ).add( index );
            }
        }

        void removeSchemaRule( long id )
        {
            if ( constraintsById.containsKey( id ) )
            {
                ConstraintDescriptor constraint = constraintsById.remove( id );
                constrainsByName.remove( constraint.getName() );
                constraints.remove( constraint );
                if ( constraint.enforcesUniqueness() )
                {
                    selectUniquenessConstraintSetByEntityType( constraint.schema().entityType() ).remove( constraint.asIndexBackedConstraint() );
                }
            }
            else if ( indexesById.containsKey( id ) )
            {
                IndexDescriptor index = indexesById.remove( id );
                SchemaDescriptor schema = index.schema();
                indexesBySchema.remove( schema );
                indexesByName.remove( index.getName(), index );
                selectIndexSetByEntityType( schema.entityType() ).remove( index );
            }
        }
    }

    /**
     * Sub-classes of the QueryCacheKey are used as memoization keys in the indexCache and constraintCache.
     * These caches hold on to the results of 'getSchemaRelatedTo' calls.
     */
    private abstract static class QueryCacheKey
    {
        private static BigInteger PRIMES = new BigInteger( "13" );

        static synchronized int nextPrime()
        {
            int result = PRIMES.intValue();
            PRIMES = PRIMES.nextProbablePrime();
            return result;
        }

        private final int hash;

        QueryCacheKey( int hash )
        {
            this.hash = hash;
        }

        static int hash( int value )
        {
            return Integer.hashCode( value );
        }

        @Override
        public int hashCode()
        {
            return hash;
        }

        @Override
        public boolean equals( Object o )
        {
            // This implementation of 'equals' is only here to satisfy checkstyle.
            throw new UnsupportedOperationException( "Equals needs to be overwritten by sub-classes." );
        }
    }

    private static class IndexesForLabelKey extends QueryCacheKey
    {
        private static final int PRIME = nextPrime();
        private final int label;

        private IndexesForLabelKey( int label )
        {
            super( hash( label ) * PRIME );
            this.label = label;
        }

        @Override
        public int hashCode()
        {
            return super.hashCode();
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            IndexesForLabelKey that = (IndexesForLabelKey) o;

            return label == that.label;
        }
    }

    private static class IndexesForRelationshipTypeKey extends QueryCacheKey
    {
        private static final int PRIME = nextPrime();
        private final int relationshipType;

        IndexesForRelationshipTypeKey( int relationshipType )
        {
            super( hash( relationshipType ) * PRIME );
            this.relationshipType = relationshipType;
        }

        @Override
        public int hashCode()
        {
            return super.hashCode();
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            IndexesForRelationshipTypeKey that = (IndexesForRelationshipTypeKey) o;

            return relationshipType == that.relationshipType;
        }
    }

    private static class IndexesRelatedToKey extends QueryCacheKey
    {
        private static final int PRIME = nextPrime();
        private final EntityType entityType;
        private final long[] changedEntityTokens;
        private final long[] unchangedEntityTokens;
        private final int[] properties;
        private final boolean propertyListIsComplete;

        IndexesRelatedToKey( EntityType entityType, long[] changedEntityTokens, long[] unchangedEntityTokens, int[] properties,
                boolean propertyListIsComplete )
        {
            this( hash( entityType, changedEntityTokens, unchangedEntityTokens, properties, propertyListIsComplete ) * PRIME,
                    entityType, changedEntityTokens, unchangedEntityTokens, properties, propertyListIsComplete );
        }

        IndexesRelatedToKey( int hash, EntityType entityType, long[] changedEntityTokens, long[] unchangedEntityTokens, int[] properties,
                boolean propertyListIsComplete )
        {
            super( hash );
            this.entityType = entityType;
            this.changedEntityTokens = changedEntityTokens;
            this.unchangedEntityTokens = unchangedEntityTokens;
            this.properties = properties;
            this.propertyListIsComplete = propertyListIsComplete;
        }

        static int hash( EntityType entityType, long[] changedEntityTokens, long[] unchangedEntityTokens, int[] properties,
                boolean propertyListIsComplete )
        {
            int result = 1;
            result = 31 * result + entityType.hashCode();
            result = 31 * result + Arrays.hashCode( changedEntityTokens );
            result = 31 * result + Arrays.hashCode( unchangedEntityTokens );
            result = 31 * result + Arrays.hashCode( properties );
            result = 31 * result + (propertyListIsComplete ? 1 : 0);
            return result;
        }

        @Override
        public int hashCode()
        {
            return super.hashCode();
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            IndexesRelatedToKey that = (IndexesRelatedToKey) o;

            if ( propertyListIsComplete != that.propertyListIsComplete )
            {
                return false;
            }
            if ( entityType != that.entityType )
            {
                return false;
            }
            if ( !Arrays.equals( changedEntityTokens, that.changedEntityTokens ) )
            {
                return false;
            }
            if ( !Arrays.equals( unchangedEntityTokens, that.unchangedEntityTokens ) )
            {
                return false;
            }
            return Arrays.equals( properties, that.properties );
        }
    }

    private static class UniqueIndexesRelatedToKey extends IndexesRelatedToKey
    {
        private static final int PRIME = nextPrime();

        UniqueIndexesRelatedToKey( EntityType entityType, long[] changedEntityTokens, long[] unchangedEntityTokens, int[] properties,
                boolean propertyListIsComplete )
        {
            super( hash( entityType, changedEntityTokens, unchangedEntityTokens, properties, propertyListIsComplete ) * PRIME,
                    entityType, changedEntityTokens, unchangedEntityTokens, properties, propertyListIsComplete );
        }
    }
}
