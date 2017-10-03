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
package org.neo4j.kernel.impl.api.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorPredicates;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static java.util.Collections.emptyIterator;

/**
 * A cache of {@link SchemaRule schema rules} as well as enforcement of schema consistency.
 * Will always reflect the committed state of the schema store.
 */
public class SchemaCache
{
    private final Lock cacheUpdateLock = new StampedLock().asWriteLock();
    private volatile SchemaCacheState schemaCacheState;

    public SchemaCache( ConstraintSemantics constraintSemantics, Iterable<SchemaRule> initialRules )
    {
        this.schemaCacheState = new SchemaCacheState( constraintSemantics, initialRules );
    }

    public Iterable<IndexRule> indexRules()
    {
        return schemaCacheState.indexRules();
    }

    public Iterable<ConstraintRule> constraintRules()
    {
        return schemaCacheState.constraintRules();
    }

    public boolean hasConstraintRule( ConstraintDescriptor descriptor )
    {
        return schemaCacheState.hasConstraintRule( descriptor );
    }

    public boolean hasIndexRule( SchemaDescriptor descriptor )
    {
        return schemaCacheState.hasIndexRule( descriptor );
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
            this.schemaCacheState = new SchemaCacheState( constraintSemantics, rules );
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

    public IndexDescriptor indexDescriptor( LabelSchemaDescriptor descriptor )
    {
        return schemaCacheState.indexDescriptor( descriptor );
    }

    public Iterator<IndexDescriptor> indexDescriptorsForLabel( int labelId )
    {
        return schemaCacheState.indexDescriptorsForLabel( labelId );
    }

    public Iterator<IndexDescriptor> indexesByProperty( int propertyId )
    {
        return schemaCacheState.indexesByProperty( propertyId );
    }

    private static class SchemaCacheState
    {
        private final ConstraintSemantics constraintSemantics;
        private final Set<ConstraintDescriptor> constraints;
        private final Map<Long,IndexRule> indexRuleById;
        private final Map<Long,ConstraintRule> constraintRuleById;

        private final Map<SchemaDescriptor,IndexDescriptor> indexDescriptors;
        private final Map<Integer,Set<IndexDescriptor>> indexDescriptorsByLabel;

        private final Map<Class<?>,Object> dependantState;
        private final Map<Integer,List<IndexDescriptor>> indexByProperty;

        SchemaCacheState( ConstraintSemantics constraintSemantics, Iterable<SchemaRule> rules )
        {
            this.constraintSemantics = constraintSemantics;
            this.constraints = new HashSet<>();
            this.indexRuleById = new HashMap<>();
            this.constraintRuleById = new HashMap<>();

            this.indexDescriptors = new HashMap<>();
            this.indexDescriptorsByLabel = new HashMap<>();
            this.dependantState = new HashMap<>();
            this.indexByProperty = new HashMap<>();
            load( rules );
        }

        SchemaCacheState( SchemaCacheState schemaCacheState )
        {
            this.constraintSemantics = schemaCacheState.constraintSemantics;
            this.indexRuleById = new HashMap<>( schemaCacheState.indexRuleById );
            this.constraintRuleById = new HashMap<>( schemaCacheState.constraintRuleById );
            this.constraints = new HashSet<>( schemaCacheState.constraints );

            this.indexDescriptors = new HashMap<>( schemaCacheState.indexDescriptors );
            this.indexDescriptorsByLabel = new HashMap<>( schemaCacheState.indexDescriptorsByLabel );
            this.dependantState = new HashMap<>();
            this.indexByProperty = new HashMap<>( schemaCacheState.indexByProperty );
        }

        public void load( Iterable<SchemaRule> schemaRuleIterator )
        {
            for ( SchemaRule schemaRule : schemaRuleIterator )
            {
                addSchemaRule( schemaRule );
            }
        }

        Iterable<IndexRule> indexRules()
        {
            return indexRuleById.values();
        }

        Iterable<ConstraintRule> constraintRules()
        {
            return constraintRuleById.values();
        }

        boolean hasConstraintRule( ConstraintDescriptor descriptor )
        {
            return constraints.contains( descriptor );
        }

        boolean hasIndexRule( SchemaDescriptor descriptor )
        {
            return indexDescriptors.containsKey( descriptor );
        }

        Iterator<ConstraintDescriptor> constraints()
        {
            return constraints.iterator();
        }

        IndexDescriptor indexDescriptor( LabelSchemaDescriptor descriptor )
        {
            return indexDescriptors.get( descriptor );
        }

        Iterator<IndexDescriptor> indexesByProperty( int propertyId )
        {
            List<IndexDescriptor> indexes = indexByProperty.get( propertyId );
            return (indexes == null) ? emptyIterator() : indexes.iterator();
        }

        Iterator<IndexDescriptor> indexDescriptorsForLabel( int labelId )
        {
            Set<IndexDescriptor> forLabel = indexDescriptorsByLabel.get( labelId );
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
            else if ( rule instanceof IndexRule )
            {
                IndexRule indexRule = (IndexRule) rule;
                indexRuleById.put( indexRule.getId(), indexRule );
                LabelSchemaDescriptor schemaDescriptor = indexRule.schema();
                IndexDescriptor indexDescriptor = indexRule.getIndexDescriptor();
                indexDescriptors.put( schemaDescriptor, indexDescriptor );

                Set<IndexDescriptor> forLabel =
                        indexDescriptorsByLabel.computeIfAbsent( schemaDescriptor.getLabelId(), k -> new HashSet<>() );
                forLabel.add( indexDescriptor );

                for ( int propertyId : indexRule.schema().getPropertyIds() )
                {
                    List<IndexDescriptor> indexesForProperty =
                            indexByProperty.computeIfAbsent( propertyId, k -> new ArrayList<>() );
                    indexesForProperty.add( indexDescriptor );
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
            else if ( indexRuleById.containsKey( id ) )
            {
                IndexRule rule = indexRuleById.remove( id );
                LabelSchemaDescriptor schema = rule.schema();
                indexDescriptors.remove( schema );

                Set<IndexDescriptor> forLabel = indexDescriptorsByLabel.get( schema.getLabelId() );
                forLabel.remove( rule.getIndexDescriptor() );
                if ( forLabel.isEmpty() )
                {
                    indexDescriptorsByLabel.remove( schema.getLabelId() );
                }

                for ( int propertyId : rule.schema().getPropertyIds() )
                {
                    List<IndexDescriptor> forProperty = indexByProperty.get( propertyId );
                    forProperty.remove( rule.getIndexDescriptor() );
                    if ( forProperty.isEmpty() )
                    {
                        indexByProperty.remove( propertyId );
                    }
                }
            }
        }
    }
}
