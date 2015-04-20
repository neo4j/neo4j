/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.NestingIterable;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.SchemaRule;

import static java.util.Collections.unmodifiableCollection;

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
    private final Map<Integer, Map<Long,SchemaRule>> rulesByLabelMap = new HashMap<>();
    private final Map<Long, SchemaRule> rulesByIdMap = new HashMap<>();

    private final Collection<UniquenessConstraint> constraints = new HashSet<>();
    private final Map<Integer, Map<Integer, CommittedIndexDescriptor>> indexDescriptors = new HashMap<>();

    public SchemaCache( Iterable<SchemaRule> initialRules )
    {
        splitUpInitialRules( initialRules );
    }

    private void splitUpInitialRules( Iterable<SchemaRule> initialRules )
    {
        for ( SchemaRule rule : initialRules )
        {
            addSchemaRule( rule );
        }
    }

    private Map<Long,SchemaRule> getOrCreateSchemaRulesMapForLabel( int label )
    {
        Map<Long,SchemaRule> rulesForLabel = rulesByLabelMap.get( label );
        if ( rulesForLabel == null )
        {
            rulesForLabel = new HashMap<>();
            rulesByLabelMap.put( label, rulesForLabel );
        }
        return rulesForLabel;
    }

    public Iterable<SchemaRule> schemaRules()
    {
        return new NestingIterable<SchemaRule, Map<Long,SchemaRule>>( rulesByLabelMap.values() )
        {
            @Override
            protected Iterator<SchemaRule> createNestedIterator( Map<Long,SchemaRule> item )
            {
                return item.values().iterator();
            }
        };
    }

    public Collection<SchemaRule> schemaRulesForLabel( int label )
    {
        Map<Long,SchemaRule> rulesForLabel = rulesByLabelMap.get( label );
        return rulesForLabel != null ? unmodifiableCollection( rulesForLabel.values() ) :
            Collections.<SchemaRule>emptyList();
    }

    public Iterator<UniquenessConstraint> constraints()
    {
        return constraints.iterator();
    }

    public Iterator<UniquenessConstraint> constraintsForLabel( final int label )
    {
        return filter( new Predicate<UniquenessConstraint>()
        {
            @Override
            public boolean accept( UniquenessConstraint item )
            {
                return item.label() == label;
            }
        }, constraints.iterator() );
    }

    public Iterator<UniquenessConstraint> constraintsForLabelAndProperty( final int label, final int property )
    {
        return filter( new Predicate<UniquenessConstraint>()
        {
            @Override
            public boolean accept( UniquenessConstraint item )
            {
                return item.label() == label && item.propertyKeyId() == property;
            }
        }, constraints.iterator() );
    }

    public void addSchemaRule( SchemaRule rule )
    {
        getOrCreateSchemaRulesMapForLabel( rule.getLabel() ).put( rule.getId(), rule );
        rulesByIdMap.put( rule.getId(), rule );

        // Note: If you start adding more unmarshalling of other types of things here,
        // make this into a more generic thing rather than adding more branch statement.
        if( rule instanceof UniquenessConstraintRule )
        {
            constraints.add( ruleToConstraint( (UniquenessConstraintRule) rule ) );
        }
        else if( rule instanceof IndexRule )
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
        rulesByLabelMap.clear();
        rulesByIdMap.clear();
        constraints.clear();
        indexDescriptors.clear();
    }

    public void load( Iterator<SchemaRule> schemaRuleIterator )
    {
        clear();
        for ( SchemaRule schemaRule : Iterables.toList( schemaRuleIterator ) )
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

        int labelId = rule.getLabel();
        Map<Long, SchemaRule> rules = rulesByLabelMap.get( labelId );
        if ( rules.remove( id ) != null && rules.isEmpty() )
        {
            rulesByLabelMap.remove( labelId );
        }

        if( rule instanceof UniquenessConstraintRule )
        {
            constraints.remove( ruleToConstraint( (UniquenessConstraintRule)rule ) );
        }
        else if( rule instanceof IndexRule )
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

    public long indexId( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        Map<Integer, CommittedIndexDescriptor> byLabel = indexDescriptors.get( index.getLabelId() );
        if ( byLabel != null )
        {
            CommittedIndexDescriptor committed = byLabel.get( index.getPropertyKeyId() );
            if ( committed != null )
            {
                return committed.getId();
            }
        }

        throw new IndexNotFoundKernelException(
            "Couldn't resolve index id for " + index + " at this point. Schema rule not committed yet?"
        );
    }

    private UniquenessConstraint ruleToConstraint( UniquenessConstraintRule constraintRule )
    {
        return new UniquenessConstraint( constraintRule.getLabel(), constraintRule.getPropertyKey() );
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

    public IndexDescriptor indexDescriptor( long indexId )
    {
        SchemaRule rule = rulesByIdMap.get( indexId );
        if ( rule instanceof IndexRule )
        {
            return indexDescriptor( rule.getLabel(), ((IndexRule) rule).getPropertyKey() );
        }
        return null;
    }
}
