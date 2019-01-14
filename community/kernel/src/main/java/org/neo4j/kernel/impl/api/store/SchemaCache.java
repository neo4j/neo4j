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
package org.neo4j.kernel.impl.api.store;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorPredicates;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.storageengine.api.schema.CapableIndexDescriptor;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

import static java.util.Collections.emptyIterator;

/**
 * A cache of {@link SchemaRule schema rules} as well as enforcement of schema consistency.
 * Will always reflect the committed state of the schema store.
 */
public class SchemaCache
{
    private final Lock cacheUpdateLock = new StampedLock().asWriteLock();
    private final IndexProviderMap indexProviderMap;
    private volatile SchemaCacheState schemaCacheState;

    public SchemaCache( ConstraintSemantics constraintSemantics, Iterable<SchemaRule> initialRules, IndexProviderMap indexProviderMap )
    {
        this.indexProviderMap = indexProviderMap;
        this.schemaCacheState = new SchemaCacheState( constraintSemantics, initialRules, indexProviderMap );
    }

    public Iterable<CapableIndexDescriptor> indexDescriptors()
    {
        return schemaCacheState.indexDescriptors();
    }

    public Iterable<ConstraintRule> constraintRules()
    {
        return schemaCacheState.constraintRules();
    }

    public boolean hasConstraintRule( Long constraintRuleId )
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

    public Iterator<ConstraintDescriptor> constraintsForLabel( final int label )
    {
        return Iterators.filter( SchemaDescriptorPredicates.hasLabel( label ), constraints() );
    }

    public Iterator<ConstraintDescriptor> constraintsForRelationshipType( final int relTypeId )
    {
        return Iterators.filter( SchemaDescriptorPredicates.hasRelType( relTypeId ), constraints() );
    }

    public Iterator<ConstraintDescriptor> constraintsForSchema( SchemaDescriptor descriptor )
    {
        return Iterators.filter( SchemaDescriptor.equalTo( descriptor ), constraints() );
    }

    public <P, T> T getOrCreateDependantState( Class<T> type, Function<P,T> factory, P parameter )
    {
        return schemaCacheState.getOrCreateDependantState( type, factory, parameter );
    }

    public void load( Iterable<SchemaRule> rules )
    {
        cacheUpdateLock.lock();
        try
        {
            ConstraintSemantics constraintSemantics = schemaCacheState.constraintSemantics;
            this.schemaCacheState = new SchemaCacheState( constraintSemantics, rules, indexProviderMap );
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

    public void removeSchemaRule( long id )
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

    public CapableIndexDescriptor indexDescriptor( SchemaDescriptor descriptor )
    {
        return schemaCacheState.indexDescriptor( descriptor );
    }

    public Iterator<CapableIndexDescriptor> indexDescriptorsForLabel( int labelId )
    {
        return schemaCacheState.indexDescriptorsForLabel( labelId );
    }

    public Iterator<CapableIndexDescriptor> indexesByProperty( int propertyId )
    {
        return schemaCacheState.indexesByProperty( propertyId );
    }

    public CapableIndexDescriptor indexDescriptorForName( String name )
    {
        return schemaCacheState.indexDescriptorByName( name );
    }

    private static class SchemaCacheState
    {
        private final ConstraintSemantics constraintSemantics;
        private final IndexProviderMap indexProviderMap;
        private final Set<ConstraintDescriptor> constraints;
        private final MutableLongObjectMap<CapableIndexDescriptor> indexDescriptorById;
        private final MutableLongObjectMap<ConstraintRule> constraintRuleById;

        private final Map<SchemaDescriptor,CapableIndexDescriptor> indexDescriptors;
        private final MutableIntObjectMap<Set<CapableIndexDescriptor>> indexDescriptorsByLabel;
        private final Map<String,CapableIndexDescriptor> indexDescriptorsByName;

        private final Map<Class<?>,Object> dependantState;
        private final MutableIntObjectMap<List<CapableIndexDescriptor>> indexByProperty;

        SchemaCacheState( ConstraintSemantics constraintSemantics, Iterable<SchemaRule> rules, IndexProviderMap indexProviderMap )
        {
            this.constraintSemantics = constraintSemantics;
            this.indexProviderMap = indexProviderMap;
            this.constraints = new HashSet<>();
            this.indexDescriptorById = new LongObjectHashMap<>();
            this.constraintRuleById = new LongObjectHashMap<>();

            this.indexDescriptors = new HashMap<>();
            this.indexDescriptorsByLabel = new IntObjectHashMap<>();
            this.indexDescriptorsByName = new HashMap<>();
            this.dependantState = new ConcurrentHashMap<>();
            this.indexByProperty = new IntObjectHashMap<>();
            load( rules );
        }

        SchemaCacheState( SchemaCacheState schemaCacheState )
        {
            this.constraintSemantics = schemaCacheState.constraintSemantics;
            this.indexDescriptorById = LongObjectHashMap.newMap( schemaCacheState.indexDescriptorById );
            this.constraintRuleById = LongObjectHashMap.newMap( schemaCacheState.constraintRuleById );
            this.constraints = new HashSet<>( schemaCacheState.constraints );

            this.indexDescriptors = new HashMap<>( schemaCacheState.indexDescriptors );
            this.indexDescriptorsByLabel = new IntObjectHashMap<>( schemaCacheState.indexDescriptorsByLabel.size() );
            schemaCacheState.indexDescriptorsByLabel.forEachKeyValue( ( k, v ) -> indexDescriptorsByLabel.put( k, new HashSet<>( v ) ) );
            this.indexDescriptorsByName = new HashMap<>( schemaCacheState.indexDescriptorsByName );
            this.dependantState = new ConcurrentHashMap<>();
            this.indexByProperty = new IntObjectHashMap<>( schemaCacheState.indexByProperty.size() );
            schemaCacheState.indexByProperty.forEachKeyValue( ( k, v ) -> indexByProperty.put( k, new ArrayList<>( v ) ) );
            this.indexProviderMap = schemaCacheState.indexProviderMap;
        }

        private void load( Iterable<SchemaRule> schemaRuleIterator )
        {
            for ( SchemaRule schemaRule : schemaRuleIterator )
            {
                addSchemaRule( schemaRule );
            }
        }

        Iterable<CapableIndexDescriptor> indexDescriptors()
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

        CapableIndexDescriptor indexDescriptor( SchemaDescriptor descriptor )
        {
            return indexDescriptors.get( descriptor );
        }

        CapableIndexDescriptor indexDescriptorByName( String name )
        {
            return indexDescriptorsByName.get( name );
        }

        Iterator<CapableIndexDescriptor> indexesByProperty( int propertyId )
        {
            List<CapableIndexDescriptor> indexes = indexByProperty.get( propertyId );
            return (indexes == null) ? emptyIterator() : indexes.iterator();
        }

        Iterator<CapableIndexDescriptor> indexDescriptorsForLabel( int labelId )
        {
            Set<CapableIndexDescriptor> forLabel = indexDescriptorsByLabel.get( labelId );
            return forLabel == null ? emptyIterator() : forLabel.iterator();
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
            else if ( rule instanceof StoreIndexDescriptor )
            {
                CapableIndexDescriptor index = indexProviderMap.withCapabilities( (StoreIndexDescriptor) rule );
                indexDescriptorById.put( index.getId(), index );
                SchemaDescriptor schemaDescriptor = index.schema();
                indexDescriptors.put( schemaDescriptor, index );
                indexDescriptorsByName.put( rule.getName(), index );
                for ( int entityTokenId : schemaDescriptor.getEntityTokenIds() )
                {
                    Set<CapableIndexDescriptor> forLabel = indexDescriptorsByLabel.getIfAbsentPut( entityTokenId, HashSet::new );
                    forLabel.add( index );
                }

                for ( int propertyId : index.schema().getPropertyIds() )
                {
                    List<CapableIndexDescriptor> indexesForProperty = indexByProperty.getIfAbsentPut( propertyId, ArrayList::new );
                    indexesForProperty.add( index );
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
                CapableIndexDescriptor index = indexDescriptorById.remove( id );
                SchemaDescriptor schema = index.schema();
                indexDescriptors.remove( schema );
                indexDescriptorsByName.remove( index.getName(), index );

                for ( int entityTokenId : schema.getEntityTokenIds() )
                {
                    Set<CapableIndexDescriptor> forLabel = indexDescriptorsByLabel.get( entityTokenId );
                    forLabel.remove( index );
                    if ( forLabel.isEmpty() )
                    {
                        indexDescriptorsByLabel.remove( entityTokenId );
                    }
                }

                for ( int propertyId : index.schema().getPropertyIds() )
                {
                    List<CapableIndexDescriptor> forProperty = indexByProperty.get( propertyId );
                    forProperty.remove( index );
                    if ( forProperty.isEmpty() )
                    {
                        indexByProperty.remove( propertyId );
                    }
                }
            }
        }
    }
}
