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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorPredicates;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
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

    public boolean hasConstraintRule( Long constraintRuleId )
    {
        return schemaCacheState.hasConstraintRule( constraintRuleId );
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

    public SchemaIndexDescriptor indexDescriptor( SchemaDescriptor descriptor )
    {
        return schemaCacheState.indexDescriptor( descriptor );
    }

    public Iterator<SchemaIndexDescriptor> indexDescriptorsForLabel( int labelId )
    {
        return schemaCacheState.indexDescriptorsForLabel( labelId );
    }

    public Iterator<SchemaIndexDescriptor> indexesByProperty( int propertyId )
    {
        return schemaCacheState.indexesByProperty( propertyId );
    }

    private static class SchemaCacheState
    {
        private final ConstraintSemantics constraintSemantics;
        private final Set<ConstraintDescriptor> constraints;
        private final PrimitiveLongObjectMap<IndexRule> indexRuleById;
        private final PrimitiveLongObjectMap<ConstraintRule> constraintRuleById;

        private final Map<SchemaDescriptor,SchemaIndexDescriptor> indexDescriptors;
        private final PrimitiveIntObjectMap<Set<SchemaIndexDescriptor>> indexDescriptorsByLabel;

        private final Map<Class<?>,Object> dependantState;
        private final PrimitiveIntObjectMap<List<SchemaIndexDescriptor>> indexByProperty;

        SchemaCacheState( ConstraintSemantics constraintSemantics, Iterable<SchemaRule> rules )
        {
            this.constraintSemantics = constraintSemantics;
            this.constraints = new HashSet<>();
            this.indexRuleById = Primitive.longObjectMap();
            this.constraintRuleById = Primitive.longObjectMap();

            this.indexDescriptors = new HashMap<>();
            this.indexDescriptorsByLabel = Primitive.intObjectMap();
            this.dependantState = new HashMap<>();
            this.indexByProperty = Primitive.intObjectMap();
            load( rules );
        }

        SchemaCacheState( SchemaCacheState schemaCacheState )
        {
            this.constraintSemantics = schemaCacheState.constraintSemantics;
            this.indexRuleById = PrimitiveLongCollections.copy( schemaCacheState.indexRuleById );
            this.constraintRuleById = PrimitiveLongCollections.copy( schemaCacheState.constraintRuleById );
            this.constraints = new HashSet<>( schemaCacheState.constraints );

            this.indexDescriptors = new HashMap<>( schemaCacheState.indexDescriptors );
            this.indexDescriptorsByLabel = PrimitiveIntCollections.copyTransform( schemaCacheState.indexDescriptorsByLabel, HashSet::new );
            this.dependantState = new HashMap<>();
            this.indexByProperty = PrimitiveIntCollections.copyTransform( schemaCacheState.indexByProperty, ArrayList::new );
        }

        private void load( Iterable<SchemaRule> schemaRuleIterator )
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

        boolean hasConstraintRule( Long constraintRuleId )
        {
            return constraintRuleId != null && constraintRuleById.containsKey( constraintRuleId );
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

        SchemaIndexDescriptor indexDescriptor( SchemaDescriptor descriptor )
        {
            return indexDescriptors.get( descriptor );
        }

        Iterator<SchemaIndexDescriptor> indexesByProperty( int propertyId )
        {
            List<SchemaIndexDescriptor> indexes = indexByProperty.get( propertyId );
            return (indexes == null) ? emptyIterator() : indexes.iterator();
        }

        Iterator<SchemaIndexDescriptor> indexDescriptorsForLabel( int labelId )
        {
            Set<SchemaIndexDescriptor> forLabel = indexDescriptorsByLabel.get( labelId );
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
                SchemaDescriptor schemaDescriptor = indexRule.schema();
                SchemaIndexDescriptor schemaIndexDescriptor = indexRule.getIndexDescriptor();
                indexDescriptors.put( schemaDescriptor, schemaIndexDescriptor );

                Set<SchemaIndexDescriptor> forLabel =
                        indexDescriptorsByLabel.computeIfAbsent( schemaDescriptor.keyId(), k -> new HashSet<>() );
                forLabel.add( schemaIndexDescriptor );

                for ( int propertyId : indexRule.schema().getPropertyIds() )
                {
                    List<SchemaIndexDescriptor> indexesForProperty =
                            indexByProperty.computeIfAbsent( propertyId, k -> new ArrayList<>() );
                    indexesForProperty.add( schemaIndexDescriptor );
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
                SchemaDescriptor schema = rule.schema();
                indexDescriptors.remove( schema );

                Set<SchemaIndexDescriptor> forLabel = indexDescriptorsByLabel.get( schema.keyId() );
                forLabel.remove( rule.getIndexDescriptor() );
                if ( forLabel.isEmpty() )
                {
                    indexDescriptorsByLabel.remove( schema.keyId() );
                }

                for ( int propertyId : rule.schema().getPropertyIds() )
                {
                    List<SchemaIndexDescriptor> forProperty = indexByProperty.get( propertyId );
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
