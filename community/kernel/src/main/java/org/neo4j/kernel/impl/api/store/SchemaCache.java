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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptor;
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

    private final Collection<NodePropertyConstraint> nodeConstraints = new HashSet<>();
    private final Collection<RelationshipPropertyConstraint> relationshipConstraints = new HashSet<>();
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
            if ( rule.getSchemaDescriptor().equals( descriptor ) )
            {
                return true;
            }
        }
        return false;
    }

    // CONSTRAINTS

    public Iterator<PropertyConstraint> constraints()
    {
        return Iterators.concat( nodeConstraints.iterator(), relationshipConstraints.iterator() );
    }

    public Iterator<NodePropertyConstraint> constraintsForLabel( final int label )
    {
        return Iterators.filter(
                constraint -> constraint.label() == label,
                nodeConstraints.iterator() );
    }

    public Iterator<NodePropertyConstraint> constraintsForLabelAndProperty( LabelSchemaDescriptor descriptor )
    {
        return Iterators.filter(
                constraint -> constraint.matches( descriptor ),
                nodeConstraints.iterator() );
    }

    public Iterator<RelationshipPropertyConstraint> constraintsForRelationshipType( final int typeId )
    {
        return Iterators.filter(
                constraint -> constraint.descriptor().getRelationshipTypeId() == typeId,
                relationshipConstraints.iterator() );
    }

    public Iterator<RelationshipPropertyConstraint> constraintsForRelationshipTypeAndProperty(
            RelationTypeSchemaDescriptor descriptor )
    {
        return Iterators.filter(
                constraint -> constraint.matches( descriptor ),
                relationshipConstraints.iterator() );
    }

    public void addSchemaRule( SchemaRule rule )
    {
        if ( rule instanceof ConstraintRule )
        {
            ConstraintRule constraintRule = (ConstraintRule) rule;
            constraintRuleById.put( constraintRule.getId(), constraintRule );
            PropertyConstraint constraint = constraintSemantics.readConstraint( constraintRule );
            if ( constraint instanceof NodePropertyConstraint )
            {
                nodeConstraints.add( (NodePropertyConstraint) constraint );
            }
            else if ( constraint instanceof RelationshipPropertyConstraint )
            {
                relationshipConstraints.add( (RelationshipPropertyConstraint) constraint );
            }
        }
        else if ( rule instanceof IndexRule )
        {
            IndexRule indexRule = (IndexRule) rule;
            indexRuleById.put( indexRule.getId(), indexRule );
            indexDescriptors.put( indexRule.getSchemaDescriptor(), indexRule.getIndexDescriptor() );
        }
    }

    public void clear()
    {
        indexRuleById.clear();
        constraintRuleById.clear();
        nodeConstraints.clear();
        relationshipConstraints.clear();
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
            PropertyConstraint constraint = constraintSemantics.readConstraint( rule );
            if ( constraint instanceof NodePropertyConstraint )
            {
                nodeConstraints.remove( constraint );
            }
            else if ( constraint instanceof RelationshipPropertyConstraint )
            {
                relationshipConstraints.remove( constraint );
            }
        }
        else if ( indexRuleById.containsKey( id ) )
        {
            IndexRule rule = indexRuleById.remove( id );
            indexDescriptors.remove( rule.getSchemaDescriptor() );
        }
    }

    public NewIndexDescriptor indexDescriptor( LabelSchemaDescriptor descriptor )
    {
        return indexDescriptors.get( descriptor );
    }
}
