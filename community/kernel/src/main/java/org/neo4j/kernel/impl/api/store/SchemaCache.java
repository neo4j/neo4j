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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptorPredicates;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintBoundary;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.storageengine.api.schema.SchemaRule;

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
    private final Map<Long, IndexRule> indexRuleById = new HashMap<>();
    private final Map<Long, ConstraintRule> constraintRuleById = new HashMap<>();
    private final Set<ConstraintDescriptor> constraints = new HashSet<>();

    private final Map<SchemaDescriptor, NewIndexDescriptor> indexDescriptors = new HashMap<>();
    private final ConstraintSemantics constraintSemantics;

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

    public void addSchemaRule( SchemaRule rule )
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
            indexDescriptors.put( indexRule.schema(), indexRule.getIndexDescriptor() );
        }
    }

    public void clear()
    {
        indexRuleById.clear();
        constraintRuleById.clear();
        constraints.clear();
        indexDescriptors.clear();
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
        if ( constraintRuleById.containsKey( id ) )
        {
            ConstraintRule rule = constraintRuleById.remove( id );
            constraints.remove( constraintSemantics.readConstraint( rule ) );
        }
        else if ( indexRuleById.containsKey( id ) )
        {
            IndexRule rule = indexRuleById.remove( id );
            indexDescriptors.remove( rule.schema() );
        }
    }

    public NewIndexDescriptor indexDescriptor( LabelSchemaDescriptor descriptor )
    {
        return indexDescriptors.get( descriptor );
    }
}
