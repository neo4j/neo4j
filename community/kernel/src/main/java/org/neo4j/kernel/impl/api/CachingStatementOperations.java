/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.util.Iterator;
import java.util.Set;

import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.EntityReadOperations;
import org.neo4j.kernel.api.operations.SchemaReadOperations;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.properties.PropertyKeyIdIterator;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.toPrimitiveLongIterator;

public class CachingStatementOperations implements
        EntityReadOperations,
        SchemaReadOperations
{
    private static final Function<? super SchemaRule, IndexDescriptor> TO_INDEX_RULE =
            new Function<SchemaRule, IndexDescriptor>()
            {
                @Override
                public IndexDescriptor apply( SchemaRule from )
                {
                    IndexRule rule = (IndexRule) from;
                    return new IndexDescriptor( rule.getLabel(), rule.getPropertyKey() );
                }
            };
    private final CacheLoader<Iterator<DefinedProperty>> nodePropertyLoader = new
            CacheLoader<Iterator<DefinedProperty>>()
            {
                @Override
                public Iterator<DefinedProperty> load( Statement state, long id ) throws EntityNotFoundException
                {
                    return entityReadDelegate.nodeGetAllProperties( state, id );
                }
            };
    private final CacheLoader<Iterator<DefinedProperty>> relationshipPropertyLoader = new
            CacheLoader<Iterator<DefinedProperty>>()
            {
                @Override
                public Iterator<DefinedProperty> load( Statement state, long id ) throws EntityNotFoundException
                {
                    return entityReadDelegate.relationshipGetAllProperties( state, id );
                }
            };
    private final CacheLoader<Iterator<DefinedProperty>> graphPropertyLoader = new
            CacheLoader<Iterator<DefinedProperty>>()
            {
                @Override
                public Iterator<DefinedProperty> load( Statement state, long id ) throws EntityNotFoundException
                {
                    return entityReadDelegate.graphGetAllProperties( state );
                }
            };
    private final CacheLoader<Set<Long>> nodeLabelLoader = new CacheLoader<Set<Long>>()
    {
        @Override
        public Set<Long> load( Statement state, long id ) throws EntityNotFoundException
        {
            return asSet( entityReadDelegate.nodeGetLabels( state, id ) );
        }
    };
    private final PersistenceCache persistenceCache;
    private final SchemaCache schemaCache;
    private final EntityReadOperations entityReadDelegate;
    private final SchemaReadOperations schemaReadDelegate;

    public CachingStatementOperations(
            EntityReadOperations entityReadDelegate,
            SchemaReadOperations schemaReadDelegate,
            PersistenceCache persistenceCache,
            SchemaCache schemaCache )
    {
        this.entityReadDelegate = entityReadDelegate;
        this.schemaReadDelegate = schemaReadDelegate;
        this.persistenceCache = persistenceCache;
        this.schemaCache = schemaCache;
    }

    @Override
    public boolean nodeHasLabel( Statement state, final long nodeId, long labelId ) throws EntityNotFoundException
    {
        return persistenceCache.nodeHasLabel( state, nodeId, labelId, nodeLabelLoader );
    }

    @Override
    public PrimitiveLongIterator nodeGetLabels( Statement state, final long nodeId ) throws EntityNotFoundException
    {
        // TODO Make PersistenceCache use primitive longs
        Iterator<Long> iterator = persistenceCache.nodeGetLabels( state, nodeId, nodeLabelLoader ).iterator();
        return toPrimitiveLongIterator( iterator );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( Statement state, long labelId )
    {
        return toIndexDescriptors( schemaCache.getSchemaRulesForLabel( labelId ), SchemaRule.Kind.INDEX_RULE );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll( Statement state )
    {
        return toIndexDescriptors( schemaCache.getSchemaRules(), SchemaRule.Kind.INDEX_RULE );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( Statement state, long labelId )
    {
        return toIndexDescriptors( schemaCache.getSchemaRulesForLabel( labelId ),
                SchemaRule.Kind.CONSTRAINT_INDEX_RULE );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll( Statement state )
    {
        return toIndexDescriptors( schemaCache.getSchemaRules(), SchemaRule.Kind.CONSTRAINT_INDEX_RULE );
    }

    private static Iterator<IndexDescriptor> toIndexDescriptors( Iterable<SchemaRule> rules,
                                                                 final SchemaRule.Kind kind )
    {
        Iterator<SchemaRule> filteredRules = filter( new Predicate<SchemaRule>()
        {
            @Override
            public boolean accept( SchemaRule item )
            {
                return item.getKind() == kind;
            }
        }, rules.iterator() );
        return map( TO_INDEX_RULE, filteredRules );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( Statement state, IndexDescriptor index ) throws
            SchemaRuleNotFoundException
    {
        IndexRule rule = indexRule( index );
        if ( rule != null )
        {
            return rule.getOwningConstraint();
        }
        return schemaReadDelegate.indexGetOwningUniquenessConstraintId( state, index );
    }

    @Override
    public long indexGetCommittedId( Statement state, IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        IndexRule rule = indexRule( index );
        if ( rule != null )
        {
            return rule.getId();
        }
        return schemaReadDelegate.indexGetCommittedId( state, index );
    }

    private IndexRule indexRule( IndexDescriptor index )
    {
        for ( SchemaRule rule : schemaCache.getSchemaRulesForLabel( index.getLabelId() ) )
        {
            if ( rule instanceof IndexRule )
            {
                IndexRule indexRule = (IndexRule) rule;
                if ( indexRule.getPropertyKey() == index.getPropertyKeyId() )
                {
                    return indexRule;
                }
            }
        }
        return null;
    }

    @Override
    public PrimitiveLongIterator nodeGetPropertyKeys( Statement state, long nodeId ) throws EntityNotFoundException
    {
        return persistenceCache.nodeGetPropertyKeys( state, nodeId, nodePropertyLoader );
    }

    @Override
    public Property nodeGetProperty( Statement state, long nodeId, long propertyKeyId ) throws EntityNotFoundException
    {
        return persistenceCache.nodeGetProperty( state, nodeId, propertyKeyId, nodePropertyLoader );
    }

    @Override
    public Iterator<DefinedProperty> nodeGetAllProperties( Statement state, long nodeId ) throws EntityNotFoundException
    {
        return persistenceCache.nodeGetProperties( state, nodeId, nodePropertyLoader );
    }

    @Override
    public PrimitiveLongIterator relationshipGetPropertyKeys( Statement state, long relationshipId )
            throws EntityNotFoundException
    {
        return new PropertyKeyIdIterator( relationshipGetAllProperties( state, relationshipId ) );
    }

    @Override
    public Property relationshipGetProperty( Statement state, long relationshipId, long propertyKeyId )
            throws EntityNotFoundException
    {
        return persistenceCache.relationshipGetProperty( state, relationshipId, propertyKeyId,
                relationshipPropertyLoader );
    }

    @Override
    public Iterator<DefinedProperty> relationshipGetAllProperties( Statement state,
                                                                   long nodeId ) throws EntityNotFoundException
    {
        return persistenceCache.relationshipGetProperties( state, nodeId, relationshipPropertyLoader );
    }

    @Override
    public PrimitiveLongIterator graphGetPropertyKeys( Statement state )
    {
        return persistenceCache.graphGetPropertyKeys( state, graphPropertyLoader );
    }

    @Override
    public Property graphGetProperty( Statement state, long propertyKeyId )
    {
        return persistenceCache.graphGetProperty( state, graphPropertyLoader, propertyKeyId );
    }

    @Override
    public Iterator<DefinedProperty> graphGetAllProperties( Statement state )
    {
        return persistenceCache.graphGetProperties( state, graphPropertyLoader );
    }

    // === TODO Below is unnecessary delegation methods

    @Override
    public long nodeGetUniqueFromIndexLookup( Statement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        return entityReadDelegate.nodeGetUniqueFromIndexLookup( state, index, value );
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel( Statement state, long labelId )
    {
        return entityReadDelegate.nodesGetForLabel( state, labelId );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexLookup( Statement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        return entityReadDelegate.nodesGetFromIndexLookup( state, index, value );
    }

    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( Statement state, long labelId, long propertyKey )
            throws SchemaRuleNotFoundException
    {
        return schemaReadDelegate.indexesGetForLabelAndPropertyKey( state, labelId, propertyKey );
    }

    @Override
    public InternalIndexState indexGetState( Statement state, IndexDescriptor descriptor ) throws
            IndexNotFoundKernelException
    {
        return schemaReadDelegate.indexGetState( state, descriptor );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( Statement state,
                                                                                long labelId, long propertyKeyId )
    {
        return schemaReadDelegate.constraintsGetForLabelAndPropertyKey( state, labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( Statement state, long labelId )
    {
        return schemaReadDelegate.constraintsGetForLabel( state, labelId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll( Statement state )
    {
        return schemaReadDelegate.constraintsGetAll( state );
    }

    // === TODO Below is unnecessary delegate methods

    @Override
    public String indexGetFailure( Statement state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return schemaReadDelegate.indexGetFailure( state, descriptor );
    }
}
