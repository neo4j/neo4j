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

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.schema.IndexDescriptor;
import org.neo4j.kernel.api.schema.IndexDescriptorFactory;
import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.schema.RelationshipPropertyDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptorPredicates;
import org.neo4j.kernel.api.schema_new.index.IndexBoundary;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static org.neo4j.helpers.collection.Iterables.filter;

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
    private final Map<Long, SchemaRule> rulesByIdMap = new HashMap<>();

    private final Collection<NodePropertyConstraint> nodeConstraints = new HashSet<>();
    private final Collection<RelationshipPropertyConstraint> relationshipConstraints = new HashSet<>();
    private final Map<IndexDescriptor, Long> indexDescriptorCommitIds = new HashMap<>();
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

    public Iterable<SchemaRule> schemaRules()
    {
        return rulesByIdMap.values();
    }

    public Iterable<IndexRule> indexRules()
    {
        return Iterables.map( IndexRule.class::cast,
                    Iterables.filter( IndexRule.class::isInstance, schemaRules() ) );
    }

    public Iterable<ConstraintRule> constraintRules()
    {
        return Iterables.map( ConstraintRule.class::cast,
                    Iterables.filter( ConstraintRule.class::isInstance, schemaRules() ) );
    }

    public Iterable<SchemaRule> schemaRulesBySchema( SchemaDescriptor descriptor )
    {
        return Iterables.filter( rule -> rule.getSchemaDescriptor().equals( descriptor ), schemaRules() );
    }

    public Iterable<SchemaRule> schemaRulesForLabel( final int label )
    {
        return filter( schemaRule -> SchemaDescriptorPredicates.hasLabel( schemaRule, label ), schemaRules() );
    }

    public Iterable<SchemaRule> schemaRulesForRelationshipType( final int relTypeId )
    {
        // warning: this previously only worked for rel type existence constraints
        return filter( schemaRule -> SchemaDescriptorPredicates.hasRelType( schemaRule, relTypeId ), schemaRules() );
    }

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

    public Iterator<NodePropertyConstraint> constraintsForLabelAndProperty( NodePropertyDescriptor descriptor )
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
            RelationshipPropertyDescriptor descriptor )
    {
        return Iterators.filter(
                constraint -> constraint.matches( descriptor ),
                relationshipConstraints.iterator() );
    }

    public void addSchemaRule( SchemaRule rule )
    {
        rulesByIdMap.put( rule.getId(), rule );

        if ( rule instanceof ConstraintRule )
        {
            PropertyConstraint constraint = constraintSemantics.readConstraint( (ConstraintRule) rule );
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
            indexDescriptorCommitIds.put( IndexBoundary.map( indexRule.getIndexDescriptor() ), indexRule.getId() );
        }
    }

    public void clear()
    {
        rulesByIdMap.clear();
        nodeConstraints.clear();
        relationshipConstraints.clear();
        indexDescriptorCommitIds.clear();
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
        SchemaRule rule = rulesByIdMap.remove( id );
        if ( rule == null )
        {
            return;
        }

        if ( rule instanceof ConstraintRule )
        {
            PropertyConstraint constraint = constraintSemantics.readConstraint( (ConstraintRule) rule );
            if ( constraint instanceof NodePropertyConstraint )
            {
                nodeConstraints.remove( constraint );
            }
            else if ( constraint instanceof RelationshipPropertyConstraint )
            {
                relationshipConstraints.remove( constraint );
            }
        }
        else if ( rule instanceof IndexRule )
        {
            IndexRule indexRule = (IndexRule) rule;
            indexDescriptorCommitIds.remove( IndexBoundary.map( indexRule.getIndexDescriptor() ) );
        }
    }

    public IndexDescriptor indexDescriptor( NodePropertyDescriptor descriptor )
    {
        IndexDescriptor key = IndexDescriptorFactory.of( descriptor );
        if ( indexDescriptorCommitIds.containsKey( key ) )
        {
            return key;
        }
        return null;
    }
}
