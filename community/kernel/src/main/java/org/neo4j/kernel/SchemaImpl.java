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
import static java.util.Collections.emptyList;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.schema.Schema.IndexState.FAILED;
import static org.neo4j.graphdb.schema.Schema.IndexState.ONLINE;
import static org.neo4j.graphdb.schema.Schema.IndexState.POPULATING;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.single;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public class SchemaImpl implements Schema
{
    private final ThreadToStatementContextBridge ctxProvider;

    public SchemaImpl( ThreadToStatementContextBridge ctxProvider )
    {
        this.ctxProvider = ctxProvider;
    }

    @Override
    public IndexCreator indexCreator( Label label )
    {
        return new IndexCreatorImpl( ctxProvider, label );
    }

    @Override
    public Iterable<IndexDefinition> getIndexes( final Label label )
    {
        StatementContext context = ctxProvider.getCtxForReading();
        try
        {
            return getIndexDefinitions( context, context.getIndexRules( context.getLabelId( label.name() ) ) );
        }
        catch ( LabelNotFoundKernelException e )
        {
            return emptyList();
        }
        finally
        {
            context.close();
        }
    }

    @Override
    public Iterable<IndexDefinition> getIndexes()
    {
        StatementContext context = ctxProvider.getCtxForReading();
        try
        {
            return getIndexDefinitions( context, context.getIndexRules() );
        }
        finally
        {
            context.close();
        }
    }
    
    private Collection<IndexDefinition> getIndexDefinitions( final StatementContext context,
                                                                   Iterator<IndexDescriptor> indexRules )
    {
        Iterator<IndexDefinition> eagerDefinitions = map( new Function<IndexDescriptor, IndexDefinition>()
        {
            @Override
            public IndexDefinition apply( IndexDescriptor rule )
            {
                try
                {
                    Label label = label( context.getLabelName( rule.getLabelId() ) );
                    String propertyKey = context.getPropertyKeyName( rule.getPropertyKeyId() );
                    return new IndexDefinitionImpl( ctxProvider, label, propertyKey );
                }
                catch ( LabelNotFoundKernelException e )
                {
                    throw new RuntimeException( e );
                }
                catch ( PropertyKeyIdNotFoundException e )
                {
                    throw new RuntimeException( e );
                }
            }
        }, indexRules );
        return asCollection( eagerDefinitions );
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
                    try
                    {
                        Thread.sleep( 100 );
                    }
                    catch ( InterruptedException e )
                    {   // What to do?
                    }
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
    
    @Override
    public ConstraintCreator constraintCreator( Label label )
    {
        return new BaseConstraintCreator( new ConstraintsActions(), label );
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints( final Label label )
    {
        final StatementContext context = ctxProvider.getCtxForReading();
        try
        {
            Iterator<UniquenessConstraint> constraints =
                    context.getConstraints( context.getLabelId( label.name() ) );
            Iterator<ConstraintDefinition> definitions =
                    map( new Function<UniquenessConstraint, ConstraintDefinition>()
            {
                @Override
                public ConstraintDefinition apply( UniquenessConstraint constraint )
                {
                    try
                    {
                        return new PropertyUniqueConstraintDefinition( new ConstraintsActions(), label,
                                context.getPropertyKeyName( constraint.property() ) );
                    }
                    catch ( PropertyKeyIdNotFoundException e )
                    {
                        throw new ThisShouldNotHappenError( "Mattias", "Couldn't find property name for " +
                                constraint.property(), e );
                    }
                }
            }, constraints );
            
            // Intentionally iterator over it so that we can close the statement context within this method
            return asCollection( definitions );
        }
        catch ( LabelNotFoundKernelException e )
        {
            return emptyList();
        }
        finally
        {
            context.close();
        }
    }
    
    private class ConstraintsActions implements InternalConstraintActions
    {
        @Override
        public ConstraintDefinition createPropertyUniquenessConstraint( Label label, String propertyKey )
        {
            return new PropertyUniqueConstraintDefinition( this, label, propertyKey );
        }

        @Override
        public void dropPropertyUniquenessConstraint( Label label, String propertyKey )
        {
            StatementContext context = ctxProvider.getCtxForWriting();
            try
            {
                long labelId = context.getOrCreateLabelId( label.name() );
                long propertyKeyId = context.getOrCreatePropertyKeyId( propertyKey );
                UniquenessConstraint constraint = context.addUniquenessConstraint( labelId, propertyKeyId );
                context.dropConstraint( constraint );
            }
            catch ( ConstraintViolationKernelException e )
            {
                throw new ThisShouldNotHappenError( "Mattias", "Unable to drop property unique constraint" );
            }
            finally
            {
                context.close();
            }
        }
    }
}
