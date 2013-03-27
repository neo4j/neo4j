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

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;

import java.util.Iterator;
import java.util.Set;

import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule.Kind;

public class CachingStatementContext extends CompositeStatementContext
{
    private static final Function<? super SchemaRule, IndexRule> TO_INDEX_RULE =
            new Function<SchemaRule, IndexRule>()
    {
        @Override
        public IndexRule apply( SchemaRule from )
        {
            return (IndexRule) from;
        }
    };
    private final PersistenceCache persistenceCache;
    private final SchemaCache schemaCache;
    private final StatementContext delegate;

    public CachingStatementContext( StatementContext actual, PersistenceCache persistenceCache, SchemaCache schemaCache )
    {
        super( actual );
        this.persistenceCache = persistenceCache;
        this.schemaCache = schemaCache;
        this.delegate = actual;
    }
    
    @Override
    public boolean addLabelToNode( long labelId, long nodeId )
    {
        Set<Long> cachedLabels = persistenceCache.getLabels( nodeId );
        if ( cachedLabels != null && cachedLabels.contains( labelId ) )
            return false;
        return delegate.addLabelToNode( labelId, nodeId );
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId )
    {
        Set<Long> labels = persistenceCache.getLabels( nodeId );
        if ( labels != null )
            return labels.contains( labelId );
        return delegate.isLabelSetOnNode( labelId, nodeId );
    }
    
    @Override
    public Iterator<Long> getLabelsForNode( long nodeId )
    {
        Set<Long> labels = persistenceCache.getLabels( nodeId );
        if ( labels != null )
            return labels.iterator();
        return delegate.getLabelsForNode( nodeId );
    }
    
    @Override
    public boolean removeLabelFromNode( long labelId, long nodeId )
    {
        Set<Long> cachedLabels = persistenceCache.getLabels( nodeId );
        if ( cachedLabels != null && !cachedLabels.contains( labelId ) )
            return false;
        return delegate.removeLabelFromNode( labelId, nodeId );
    }

    @Override
    public Iterator<IndexRule> getIndexRules( long labelId )
    {
        return toIndexRules( schemaCache.getSchemaRules( labelId ) );
    }

    @Override
    public Iterator<IndexRule> getIndexRules()
    {
        return toIndexRules( schemaCache.getSchemaRules() );
    }

    private Iterator<IndexRule> toIndexRules( Iterable<SchemaRule> schemaRules )
    {
        Iterator<SchemaRule> filteredRules = filter( new Predicate<SchemaRule>()
        {
            @Override
            public boolean accept( SchemaRule item )
            {
                return item.getKind() == Kind.INDEX_RULE;
            }
        }, schemaRules.iterator() );
        return map( TO_INDEX_RULE, filteredRules );
    }
}
