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
package org.neo4j.kernel;

import static java.util.Collections.emptyList;
import static org.neo4j.helpers.collection.Iterables.map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.core.KeyHolder;
import org.neo4j.kernel.impl.core.PropertyIndex;

public class SchemaImpl implements Schema
{
    private final ThreadToStatementContextBridge ctxProvider;
    private final KeyHolder<PropertyIndex> propertyKeyManager;

    public SchemaImpl( ThreadToStatementContextBridge ctxProvider, KeyHolder<PropertyIndex> propertyKeyManager )
    {
        this.ctxProvider = ctxProvider;
        this.propertyKeyManager = propertyKeyManager;
    }

    @Override
    public IndexCreator indexCreator( Label label )
    {
        return new IndexCreatorImpl( ctxProvider, propertyKeyManager, label );
    }

    @Override
    public Iterable<IndexDefinition> getIndexes( final Label label )
    {
        StatementContext context = ctxProvider.getCtxForReading();
        try
        {
            return map( new Function<Long, IndexDefinition>()
            {
                @Override
                public IndexDefinition apply( Long propertyKey )
                {
                    return new IndexDefinitionImpl( ctxProvider, label,
                            propertyKeyManager.getKeyByIdOrNull( propertyKey.intValue() ).getKey() );
                }
            }, context.getIndexRules( context.getLabelId( label.name() ) ) );
        }
        catch ( LabelNotFoundKernelException e )
        {
            return emptyList();
        }
    }
}
