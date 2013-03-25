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

import static java.lang.String.format;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.schema.Schema.IndexState.FAILED;
import static org.neo4j.graphdb.schema.Schema.IndexState.ONLINE;
import static org.neo4j.graphdb.schema.Schema.IndexState.POPULATING;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.helpers.collection.IteratorUtil.withResource;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.impl.core.KeyHolder;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

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
    public ResourceIterable<IndexDefinition> getIndexes( final Label label )
    {
        return new ResourceIterable<IndexDefinition>()
        {
            @Override
            public ResourceIterator<IndexDefinition> iterator()
            {
                StatementContext context = ctxProvider.getCtxForReading();
                try
                {
                    return getIndexDefinitions( context, context.getIndexRules( context.getLabelId( label.name() ) ) );
                }
                catch ( LabelNotFoundKernelException e )
                {
                    context.close();
                    return emptyIterator();
                }
            }
        };
    }

    @Override
    public ResourceIterable<IndexDefinition> getIndexes()
    {
        return new ResourceIterable<IndexDefinition>()
        {
            @Override
            public ResourceIterator<IndexDefinition> iterator()
            {
                StatementContext context = ctxProvider.getCtxForReading();
                return getIndexDefinitions( context, context.getIndexRules() );
            }
        };
    }
    
    private ResourceIterator<IndexDefinition> getIndexDefinitions( final StatementContext context,
                                                                   Iterator<IndexRule> indexRules )
    {
        return withResource( map( new Function<IndexRule, IndexDefinition>()
        {
            @Override
            public IndexDefinition apply( IndexRule rule )
            {
                try
                {
                    return new IndexDefinitionImpl( ctxProvider, label( context.getLabelName( rule.getLabel() ) ),
                            propertyKeyManager.getKeyByIdOrNull( (int) rule.getPropertyKey() ).getKey() );
                }
                catch ( LabelNotFoundKernelException e )
                {
                    throw new RuntimeException( e );
                }
            }
        }, indexRules ), context );
    }

    @Override
    public void awaitIndexOnline( IndexDefinition index, long duration, TimeUnit unit )
    {
        long now     = System.currentTimeMillis();
        long timeout = now + unit.toMillis( duration );
        do
        {
            IndexState state = getIndexState( index );
            switch (state)
            {
                case ONLINE:
                    return;
                case FAILED:
                    throw new IllegalStateException( "Index entered a FAILED state. Please see database logs." );
                default:
                    Thread.yield();
                    break;
            }
        } while ( System.currentTimeMillis() < timeout );
        throw new IllegalStateException( "Expected index to come online within a reasonable time." );
    }

    @Override
    public IndexState getIndexState( IndexDefinition index )
    {
        StatementContext context = ctxProvider.getCtxForReading();
        String propertyKey = single( index.getPropertyKeys() );
        try
        {
            long labelId = context.getLabelId( index.getLabel().name() );
            long propertyKeyId = context.getPropertyKeyId( propertyKey );
            org.neo4j.kernel.api.index.InternalIndexState indexState =
                    context.getIndexState( context.getIndexRule( labelId, propertyKeyId ) );
            switch ( indexState )
            {
                case POPULATING:
                    return POPULATING;
                case ONLINE:
                    return ONLINE;
                case FAILED:
                    return FAILED;
                default:
                    throw new IllegalArgumentException( String.format( "Illegal index state %s", indexState ) );
            }
        }
        catch ( LabelNotFoundKernelException e )
        {
            throw new NotFoundException( format( "Label %s not found", index.getLabel().name() ) );
        }
        catch ( PropertyKeyNotFoundException e )
        {
            throw new NotFoundException( format( "Property key %s not found", propertyKey ) );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            throw new NotFoundException( format( "No index for label %s on property %s",
                    index.getLabel().name(), propertyKey ) );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new NotFoundException( format( "No index for label %s on property %s",
                    index.getLabel().name(), propertyKey ), e );
        }
        finally
        {
            context.close();
        }
    }
}
