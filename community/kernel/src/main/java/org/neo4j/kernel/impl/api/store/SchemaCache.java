/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.function.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodePropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.PropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.RelationshipPropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.SchemaRule;

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
    private final Map<Integer, Map<Integer, CommittedIndexDescriptor>> indexDescriptors = new HashMap<>();
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

    public Iterable<SchemaRule> schemaRulesForLabel( final int label )
    {
        return filter( new Predicate<SchemaRule>()
        {
            @Override
            public boolean test( SchemaRule schemaRule )
            {
                return schemaRule.getKind() != SchemaRule.Kind.RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT &&
                       schemaRule.getLabel() == label;
            }
        }, schemaRules() );
    }

    public Iterable<SchemaRule> schemaRulesForRelationshipType( final int typeId )
    {
        return filter( new Predicate<SchemaRule>()
        {
            @Override
            public boolean test( SchemaRule schemaRule )
            {
                return schemaRule.getKind() == SchemaRule.Kind.RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT &&
                       schemaRule.getRelationshipType() == typeId;
            }
        }, schemaRules() );
    }

    public Iterator<PropertyConstraint> constraints()
    {
        return Iterables.concat( nodeConstraints.iterator(), relationshipConstraints.iterator() );
    }

    public Iterator<NodePropertyConstraint> constraintsForLabel( final int label )
    {
        return filter( new Predicate<NodePropertyConstraint>()
        {
            @Override
            public boolean test( NodePropertyConstraint constraint )
            {
                return constraint.label() == label;
            }
        }, nodeConstraints.iterator() );
    }

    public Iterator<NodePropertyConstraint> constraintsForLabelAndProperty( final int label, final int property )
    {
        return filter( new Predicate<NodePropertyConstraint>()
        {
            @Override
            public boolean test( NodePropertyConstraint constraint )
            {
                return constraint.label() == label && constraint.propertyKey() == property;
            }
        }, nodeConstraints.iterator() );
    }

    public Iterator<RelationshipPropertyConstraint> constraintsForRelationshipType( final int typeId )
    {
        return filter( new Predicate<RelationshipPropertyConstraint>()
        {
            @Override
            public boolean test( RelationshipPropertyConstraint constraint )
            {
                return constraint.relationshipType() == typeId;
            }
        }, relationshipConstraints.iterator() );
    }

    public Iterator<RelationshipPropertyConstraint> constraintsForRelationshipTypeAndProperty( final int typeId,
            final int propertyKeyId )
    {
        return filter( new Predicate<RelationshipPropertyConstraint>()
        {
            @Override
            public boolean test( RelationshipPropertyConstraint constraint )
            {
                return constraint.relationshipType() == typeId && constraint.propertyKey() == propertyKeyId;
            }
        }, relationshipConstraints.iterator() );
    }

    public void addSchemaRule( SchemaRule rule )
    {
        rulesByIdMap.put( rule.getId(), rule );

        if ( rule instanceof PropertyConstraintRule )
        {
            PropertyConstraint constraint = constraintSemantics.readConstraint( (PropertyConstraintRule) rule );
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
            Map<Integer, CommittedIndexDescriptor> byLabel = indexDescriptors.get( indexRule.getLabel() );
            if ( byLabel == null )
            {
                indexDescriptors.put( indexRule.getLabel(), byLabel = new HashMap<>() );
            }
            byLabel.put( indexRule.getPropertyKey(), new CommittedIndexDescriptor( indexRule.getLabel(),
                    indexRule.getPropertyKey(), indexRule.getId() ) );
        }
    }

    public void clear()
    {
        rulesByIdMap.clear();
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

    // We could have had this class extend IndexDescriptor instead. That way we could have gotten the id
    // from an IndexDescriptor instance directly. The problem is that it would only work for index descriptors
    // instantiated by a SchemaCache. Perhaps that is always the case. Anyways, doing it like that resulted
    // in unit test failures regarding the schema cache, so this way (the wrapping way) is a more generic
    // and stable way of doing it.
    private static class CommittedIndexDescriptor
    {
        private final IndexDescriptor descriptor;
        private final long id;

        public CommittedIndexDescriptor( int labelId, int propertyKey, long id )
        {
            this.descriptor = new IndexDescriptor( labelId, propertyKey );
            this.id = id;
        }

        public IndexDescriptor getDescriptor()
        {
            return descriptor;
        }

        public long getId()
        {
            return id;
        }
    }

    public void removeSchemaRule( long id )
    {
        SchemaRule rule = rulesByIdMap.remove( id );
        if ( rule == null )
        {
            return;
        }

        if ( rule instanceof NodePropertyConstraintRule )
        {
            nodeConstraints.remove( ((NodePropertyConstraintRule) rule).toConstraint() );
        }
        else if ( rule instanceof RelationshipPropertyConstraintRule )
        {
            relationshipConstraints.remove( ((RelationshipPropertyConstraintRule) rule).toConstraint() );
        }
        else if ( rule instanceof IndexRule )
        {
            IndexRule indexRule = (IndexRule) rule;
            Map<Integer, CommittedIndexDescriptor> byLabel = indexDescriptors.get( indexRule.getLabel() );
            byLabel.remove( indexRule.getPropertyKey() );
            if ( byLabel.isEmpty() )
            {
                indexDescriptors.remove( indexRule.getLabel() );
            }
        }
    }

    public IndexDescriptor indexDescriptor( int labelId, int propertyKey )
    {
        Map<Integer, CommittedIndexDescriptor> byLabel = indexDescriptors.get( labelId );
        if ( byLabel != null )
        {
            CommittedIndexDescriptor committed = byLabel.get( propertyKey );
            if ( committed != null )
            {
                return committed.getDescriptor();
            }
        }
        return null;
    }
}
