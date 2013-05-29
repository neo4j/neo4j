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
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class CachingStatementContext extends StoreOperationTranslatingStatementContext
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
    private final CacheLoader<Iterator<Property>> nodePropertyLoader = new CacheLoader<Iterator<Property>>()
    {
        @Override
        public Iterator<Property> load( long id ) throws EntityNotFoundException
        {
            return delegate.nodeGetAllProperties( id );
        }
    };
    private final CacheLoader<Iterator<Property>> relationshipPropertyLoader = new CacheLoader<Iterator<Property>>()
    {
        @Override
        public Iterator<Property> load( long id ) throws EntityNotFoundException
        {
            return delegate.relationshipGetAllProperties( id );
        }
    };
    private final CacheLoader<Iterator<Property>> graphPropertyLoader = new CacheLoader<Iterator<Property>>()
    {
        @Override
        public Iterator<Property> load( long id ) throws EntityNotFoundException
        {
            return delegate.graphGetAllProperties();
        }
    };
    private final CacheLoader<Set<Long>> nodeLabelLoader = new CacheLoader<Set<Long>>()
    {
        @Override
        public Set<Long> load( long id ) throws EntityNotFoundException
        {
            return asSet( delegate.nodeGetLabels( id ) );
        }
    };
    private final PersistenceCache persistenceCache;
    private final SchemaCache schemaCache;
    private final StatementContext delegate;

    public CachingStatementContext( StatementContext actual, PersistenceCache persistenceCache,
                                    SchemaCache schemaCache )
    {
        super( actual );
        this.persistenceCache = persistenceCache;
        this.schemaCache = schemaCache;
        this.delegate = actual;
    }

    @Override
    public boolean nodeHasLabel( final long nodeId, long labelId ) throws EntityNotFoundException
    {
        return persistenceCache.nodeHasLabel( nodeId, labelId, nodeLabelLoader );
    }

    @Override
    public Iterator<Long> nodeGetLabels( final long nodeId ) throws EntityNotFoundException
    {
        return persistenceCache.nodeGetLabels( nodeId, nodeLabelLoader ).iterator();
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( long labelId )
    {
        return toIndexDescriptors( schemaCache.getSchemaRulesForLabel( labelId ), SchemaRule.Kind.INDEX_RULE );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        return toIndexDescriptors( schemaCache.getSchemaRules(), SchemaRule.Kind.INDEX_RULE );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( long labelId )
    {
        return toIndexDescriptors( schemaCache.getSchemaRulesForLabel( labelId ),
                                   SchemaRule.Kind.CONSTRAINT_INDEX_RULE );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll()
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
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        IndexRule rule = indexRule( index );
        if ( rule != null )
        {
            return rule.getOwningConstraint();
        }
        return delegate.indexGetOwningUniquenessConstraintId( index );
    }

    @Override
    public long indexGetCommittedId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        IndexRule rule = indexRule( index );
        if ( rule != null )
        {
            return rule.getId();
        }
        return delegate.indexGetCommittedId( index );
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
    public Iterator<Property> nodeGetAllProperties( long nodeId ) throws EntityNotFoundException
    {
        return persistenceCache.nodeGetProperties( nodeId, nodePropertyLoader );
    }
    
    @Override
    public Iterator<Property> relationshipGetAllProperties( long nodeId ) throws EntityNotFoundException
    {
        return persistenceCache.relationshipGetProperties( nodeId, relationshipPropertyLoader );
    }
    
    @Override
    public Iterator<Property> graphGetAllProperties()
    {
        return persistenceCache.graphGetProperties( graphPropertyLoader );
    }
}
