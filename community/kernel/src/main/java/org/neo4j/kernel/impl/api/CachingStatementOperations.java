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

import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.api.KernelStatement;
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
import static org.neo4j.kernel.impl.api.PrimitiveIntIteratorForArray.primitiveIntIteratorToIntArray;

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
            // We know that we only have int range of property key ids.
            return new IndexDescriptor( rule.getLabel(), rule.getPropertyKey() );
        }
    };
    private final CacheLoader<Iterator<DefinedProperty>> nodePropertyLoader = new CacheLoader<Iterator<DefinedProperty>>()
    {
        @Override
        public Iterator<DefinedProperty> load( KernelStatement state, long id ) throws EntityNotFoundException
        {
            return entityReadDelegate.nodeGetAllProperties( state, id );
        }
    };
    private final CacheLoader<Iterator<DefinedProperty>> relationshipPropertyLoader = new CacheLoader<Iterator<DefinedProperty>>()
    {
        @Override
        public Iterator<DefinedProperty> load( KernelStatement state, long id ) throws EntityNotFoundException
        {
            return entityReadDelegate.relationshipGetAllProperties( state, id );
        }
    };
    private final CacheLoader<Iterator<DefinedProperty>> graphPropertyLoader = new CacheLoader<Iterator<DefinedProperty>>()
    {
        @Override
        public Iterator<DefinedProperty> load( KernelStatement state, long id ) throws EntityNotFoundException
        {
            return entityReadDelegate.graphGetAllProperties(state);
        }
    };
    private final CacheLoader<int[]> nodeLabelLoader = new CacheLoader<int[]>()
    {
        @Override
        public int[] load( KernelStatement state, long id ) throws EntityNotFoundException
        {
            return primitiveIntIteratorToIntArray( entityReadDelegate.nodeGetLabels( state, id ) );
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
    public boolean nodeHasLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException
    {
        return persistenceCache.nodeHasLabel( state, nodeId, labelId, nodeLabelLoader );
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels( KernelStatement state, long nodeId ) throws EntityNotFoundException
    {
        return new PrimitiveIntIteratorForArray( persistenceCache.nodeGetLabels( state, nodeId, nodeLabelLoader ) );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( KernelStatement state, int labelId )
    {
        return toIndexDescriptors( schemaCache.schemaRulesForLabel( labelId ), SchemaRule.Kind.INDEX_RULE );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll( KernelStatement state )
    {
        return toIndexDescriptors( schemaCache.schemaRules(), SchemaRule.Kind.INDEX_RULE );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( KernelStatement state, int labelId )
    {
        return toIndexDescriptors( schemaCache.schemaRulesForLabel( labelId ),
                SchemaRule.Kind.CONSTRAINT_INDEX_RULE );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll( KernelStatement state )
    {
        return toIndexDescriptors( schemaCache.schemaRules(), SchemaRule.Kind.CONSTRAINT_INDEX_RULE );
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
    public Long indexGetOwningUniquenessConstraintId( KernelStatement state, IndexDescriptor index )
            throws SchemaRuleNotFoundException
    {
        IndexRule rule = indexRule( index );
        if ( rule != null )
        {
            return rule.getOwningConstraint();
        }
        return schemaReadDelegate.indexGetOwningUniquenessConstraintId( state, index );
    }

    @Override
    public long indexGetCommittedId( KernelStatement state, IndexDescriptor index ) throws SchemaRuleNotFoundException
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
        for ( SchemaRule rule : schemaCache.schemaRulesForLabel( index.getLabelId() ) )
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
    public PrimitiveLongIterator nodeGetPropertyKeys( KernelStatement state, long nodeId ) throws EntityNotFoundException
    {
        return persistenceCache.nodeGetPropertyKeys( state, nodeId, nodePropertyLoader );
    }

    @Override
    public Property nodeGetProperty( KernelStatement state, long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        return persistenceCache.nodeGetProperty( state, nodeId, propertyKeyId, nodePropertyLoader );
    }

    @Override
    public Iterator<DefinedProperty> nodeGetAllProperties( KernelStatement state, long nodeId ) throws EntityNotFoundException
    {
        return persistenceCache.nodeGetProperties( state, nodeId, nodePropertyLoader );
    }

    @Override
    public PrimitiveLongIterator relationshipGetPropertyKeys( KernelStatement state, long relationshipId )
            throws EntityNotFoundException
    {
        return new PropertyKeyIdIterator( relationshipGetAllProperties( state, relationshipId ) );
    }

    @Override
    public Property relationshipGetProperty( KernelStatement state, long relationshipId, int propertyKeyId )
            throws EntityNotFoundException
    {
        return persistenceCache.relationshipGetProperty( state, relationshipId, propertyKeyId,
                relationshipPropertyLoader );
    }

    @Override
    public Iterator<DefinedProperty> relationshipGetAllProperties( KernelStatement state, long nodeId )
            throws EntityNotFoundException
    {
        return persistenceCache.relationshipGetProperties( state, nodeId, relationshipPropertyLoader );
    }

    @Override
    public PrimitiveLongIterator graphGetPropertyKeys( KernelStatement state )
    {
        return persistenceCache.graphGetPropertyKeys( state, graphPropertyLoader );
    }

    @Override
    public Property graphGetProperty( KernelStatement state, int propertyKeyId )
    {
        return persistenceCache.graphGetProperty( state, graphPropertyLoader, propertyKeyId );
    }

    @Override
    public Iterator<DefinedProperty> graphGetAllProperties( KernelStatement state )
    {
        return persistenceCache.graphGetProperties( state, graphPropertyLoader );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey(
            KernelStatement state, int labelId, int propertyKeyId )
    {
        return schemaCache.constraintsForLabelAndProperty( labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( KernelStatement state, int labelId )
    {
        return schemaCache.constraintsForLabel( labelId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll( KernelStatement state )
    {
        return schemaCache.constraints();
    }

    // === TODO Below is unnecessary delegation methods

    @Override
    public long nodeGetUniqueFromIndexLookup( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        return entityReadDelegate.nodeGetUniqueFromIndexLookup( state, index, value );
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel( KernelStatement state, int labelId )
    {
        return entityReadDelegate.nodesGetForLabel( state, labelId );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexLookup( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        return entityReadDelegate.nodesGetFromIndexLookup( state, index, value );
    }

    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( KernelStatement state, int labelId, int propertyKey )
            throws SchemaRuleNotFoundException
    {
        return schemaReadDelegate.indexesGetForLabelAndPropertyKey( state, labelId, propertyKey );
    }

    @Override
    public InternalIndexState indexGetState( KernelStatement state, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        return schemaReadDelegate.indexGetState( state, descriptor );
    }

    // === TODO Below is unnecessary delegate methods

    @Override
    public String indexGetFailure( Statement state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return schemaReadDelegate.indexGetFailure( state, descriptor );
    }
}
