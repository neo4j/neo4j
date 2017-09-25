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

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import static java.util.Collections.newSetFromMap;

/**
 * A cache of {@link SchemaRule schema rules} as well as enforcement of schema consistency.
 * Will always reflect the committed state of the schema store.
 *
 * Assume synchronization/locking is done outside, with locks.
 *
 * @author Mattias Persson
 * @author Stefan Plantikow
 */
public class SchemaCache
{
    private final Map<Long,IndexRule> indexRuleById = new ConcurrentHashMap<>();
    private final Map<Long,ConstraintRule> constraintRuleById = new ConcurrentHashMap<>();
    private final Set<ConstraintDescriptor> constraints = newSetFromMap( new ConcurrentHashMap<>() );

    private final Map<SchemaDescriptor,IndexDescriptor> indexDescriptors = new ConcurrentHashMap<>();
    private final Map<Integer,Set<IndexDescriptor>> indexDescriptorsByLabel = new ConcurrentHashMap<>();
    private final ConstraintSemantics constraintSemantics;

    private final Map<Class<?>,Object> dependantState = new ConcurrentHashMap<>();
    private final Map<Integer,List<IndexDescriptor>> indexByProperty = new ConcurrentHashMap<>();

    public SchemaCache( ConstraintSemantics constraintSemantics, Iterable<SchemaRule> initialRules )
    {
        this.constraintSemantics = constraintSemantics;
        splitUpInitialRules( initialRules );
    }

    private void splitUpInitialRules( Iterable<SchemaRule> initialRules )
    {
        for ( SchemaRule rule : initialRules )
        {
            addSchemaRule( rule );
        }
    }

    // SCHEMA RULES

    public Iterable<IndexRule> indexRules()
    {
        return indexRuleById.values();
    }

    public Iterable<ConstraintRule> constraintRules()
    {
        return constraintRuleById.values();
    }

    public boolean hasConstraintRule( ConstraintDescriptor descriptor )
    {
        for ( ConstraintRule rule : constraintRuleById.values() )
        {
            if ( rule.getConstraintDescriptor().equals( descriptor ) )
            {
                return true;
            }
        }
        return false;
    }

    public boolean hasIndexRule( SchemaDescriptor descriptor )
    {
        for ( IndexRule rule : indexRuleById.values() )
        {
            if ( rule.schema().equals( descriptor ) )
            {
                return true;
            }
        }
        return false;
    }

    // CONSTRAINTS

    public Iterator<ConstraintDescriptor> constraints()
    {
        return constraints.iterator();
    }

    public Iterator<ConstraintDescriptor> constraintsForLabel( final int label )
    {
        return Iterators.filter( SchemaDescriptorPredicates.hasLabel( label ), constraints.iterator() );
    }

    public Iterator<ConstraintDescriptor> constraintsForRelationshipType( final int relTypeId )
    {
        return Iterators.filter( SchemaDescriptorPredicates.hasRelType( relTypeId ), constraints.iterator() );
    }

    public Iterator<ConstraintDescriptor> constraintsForSchema( SchemaDescriptor descriptor )
    {
        return Iterators.filter( SchemaDescriptor.equalTo( descriptor ), constraints.iterator() );
    }

    public <P, T> T getOrCreateDependantState( Class<T> type, Function<P,T> factory, P parameter )
    {
        return type.cast( dependantState.computeIfAbsent( type, key -> factory.apply( parameter ) ) );
    }

    public void addSchemaRule( SchemaRule rule )
    {
        dependantState.clear();
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
            LabelSchemaDescriptor schema = indexRule.schema();
            indexDescriptors.put( schema, indexRule.getIndexDescriptor() );

            Set<IndexDescriptor> forLabel = indexDescriptorsByLabel.get( schema.getLabelId() );
            if ( forLabel == null )
            {
                forLabel = new HashSet<>();
                indexDescriptorsByLabel.put( schema.getLabelId(), forLabel );
            }
            forLabel.add( indexRule.getIndexDescriptor() );

            for ( int propertyId : indexRule.schema().getPropertyIds() )
            {
                List<IndexDescriptor> indexesForProperty = indexByProperty.get( propertyId );
                if ( indexesForProperty == null )
                {
                    indexesForProperty = new LinkedList<>();
                    indexByProperty.put( propertyId, indexesForProperty );
                }
                indexesForProperty.add( indexRule.getIndexDescriptor() );
            }
        }
    }

    public void clear()
    {
        indexRuleById.clear();
        constraintRuleById.clear();
        constraints.clear();
        indexDescriptors.clear();
        indexByProperty.clear();
    }

    public void load( List<SchemaRule> schemaRuleIterator )
    {
        clear();
        for ( SchemaRule schemaRule : schemaRuleIterator )
        {
            addSchemaRule( schemaRule );
        }
    }

    public void removeSchemaRule( long id )
    {
        dependantState.clear();
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

    public IndexDescriptor indexDescriptor( LabelSchemaDescriptor descriptor )
    {
        return indexDescriptors.get( descriptor );
    }

    public Iterator<IndexDescriptor> indexDescriptorsForLabel( int labelId )
    {
        Set<IndexDescriptor> forLabel = indexDescriptorsByLabel.get( labelId );
        return forLabel == null ? emptyIterator() : forLabel.iterator();
    }

    public Iterator<IndexDescriptor> indexesByProperty( int propertyId )
    {
        List<IndexDescriptor> indexes = indexByProperty.get( propertyId );
        return (indexes == null) ? emptyIterator() : indexes.iterator();
    }
}
