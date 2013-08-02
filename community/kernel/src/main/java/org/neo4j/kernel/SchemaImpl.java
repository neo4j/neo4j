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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.operations.KeyNameLookup;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.schema.Schema.IndexState.FAILED;
import static org.neo4j.graphdb.schema.Schema.IndexState.ONLINE;
import static org.neo4j.graphdb.schema.Schema.IndexState.POPULATING;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.single;

public class SchemaImpl implements Schema
{
    private final ThreadToStatementContextBridge ctxProvider;
    private final InternalSchemaActions actions;

    public SchemaImpl( ThreadToStatementContextBridge ctxProvider )
    {
        this.ctxProvider = ctxProvider;
        this.actions = new GDBSchemaActions( ctxProvider );
    }

    @Override
    public IndexCreator indexFor( Label label )
    {
        assertInTransaction();

        return new IndexCreatorImpl( actions, label );
    }

    @Override
    public Iterable<IndexDefinition> getIndexes( final Label label )
    {
        assertInTransaction();

        StatementOperationParts context = ctxProvider.getCtxForReading();
        StatementState state = ctxProvider.statementForReading();
        try
        {
            List<IndexDefinition> definitions = new ArrayList<>();
            long labelId = context.keyReadOperations().labelGetForName( state, label.name() );
            addDefinitions( definitions, context, state, context.schemaReadOperations().indexesGetForLabel( state, labelId ), false );
            addDefinitions( definitions, context, state, context.schemaReadOperations().uniqueIndexesGetForLabel( state, labelId ), true );
            return definitions;
        }
        catch ( LabelNotFoundKernelException e )
        {
            return emptyList();
        }
        finally
        {
            state.close();
        }
    }

    @Override
    public Iterable<IndexDefinition> getIndexes()
    {
        assertInTransaction();

        StatementOperationParts context = ctxProvider.getCtxForReading();
        StatementState state = ctxProvider.statementForReading();
        try
        {
            List<IndexDefinition> definitions = new ArrayList<>();
            addDefinitions( definitions, context, state, context.schemaReadOperations().indexesGetAll( state ), false );
            addDefinitions( definitions, context, state, context.schemaReadOperations().uniqueIndexesGetAll( state ), true );
            return definitions;
        }
        finally
        {
            state.close();
        }
    }

    private void addDefinitions( List<IndexDefinition> definitions, final StatementOperationParts context,
            final StatementState state, Iterator<IndexDescriptor> indexes, final boolean constraintIndex )
    {
        addToCollection( map( new Function<IndexDescriptor, IndexDefinition>()
        {
            @Override
            public IndexDefinition apply( IndexDescriptor rule )
            {
                try
                {
                    Label label = label( context.keyReadOperations().labelGetName( state, rule.getLabelId() ) );
                    String propertyKey = context.keyReadOperations().propertyKeyGetName( state, rule.getPropertyKeyId() );
                    return new IndexDefinitionImpl( actions, label, propertyKey, constraintIndex );
                }
                catch ( LabelNotFoundKernelException | PropertyKeyIdNotFoundException e )
                {
                    throw new RuntimeException( e );
                }
            }
        }, indexes ), definitions );
    }

    @Override
    public void awaitIndexOnline( IndexDefinition index, long duration, TimeUnit unit )
    {
        assertInTransaction();

        long now = System.currentTimeMillis();
        long timeout = now + unit.toMillis( duration );
        do
        {
            IndexState state = getIndexState( index );
            switch ( state )
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
    public void awaitIndexesOnline( long duration, TimeUnit unit )
    {
        assertInTransaction();

        long millisLeft = TimeUnit.MILLISECONDS.convert( duration, unit );
        Collection<IndexDefinition> onlineIndexes = new ArrayList<>();

        for ( Iterator<IndexDefinition> iter = getIndexes().iterator(); iter.hasNext(); )
        {
            if ( millisLeft < 0 )
                throw new IllegalStateException( "Expected all indexes to come online within a reasonable time."
                                                 + "Indexes brought online: " + onlineIndexes
                                                 + ". Indexes not guaranteed to be online: " + asCollection( iter ) );

            IndexDefinition index = iter.next();
            long millisBefore = System.currentTimeMillis();
            awaitIndexOnline( index, millisLeft, TimeUnit.MILLISECONDS );
            millisLeft -= System.currentTimeMillis() - millisBefore;

            onlineIndexes.add( index );
        }
    }

    @Override
    public IndexState getIndexState( final IndexDefinition index )
    {
        assertInTransaction();

        StatementOperationParts context = ctxProvider.getCtxForReading();
        String propertyKey = single( index.getPropertyKeys() );
        StatementState state = ctxProvider.statementForReading();
        try
        {
            long labelId = context.keyReadOperations().labelGetForName( state, index.getLabel().name() );
            long propertyKeyId = context.keyReadOperations().propertyKeyGetForName( state, propertyKey );
            org.neo4j.kernel.api.index.InternalIndexState indexState = context.schemaReadOperations().indexGetState( state,
                    context.schemaReadOperations().indexesGetForLabelAndPropertyKey( state, labelId, propertyKeyId ) );
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
        catch ( SchemaRuleNotFoundException | IndexNotFoundKernelException e )
        {
            throw new NotFoundException( format( "No index for label %s on property %s",
                                                 index.getLabel().name(), propertyKey ) );
        }
        finally
        {
            state.close();
        }
    }

    @Override
    public String getIndexFailure( IndexDefinition index )
    {
        assertInTransaction();

        StatementOperationParts context = ctxProvider.getCtxForReading();
        StatementState state = ctxProvider.statementForReading();
        String propertyKey = single( index.getPropertyKeys() );
        try
        {
            long labelId = context.keyReadOperations().labelGetForName( state, index.getLabel().name() );
            long propertyKeyId = context.keyReadOperations().propertyKeyGetForName( state, propertyKey );
            IndexDescriptor indexId = context.schemaReadOperations().indexesGetForLabelAndPropertyKey( state, labelId, propertyKeyId );
            return context.schemaReadOperations().indexGetFailure( state, indexId );
        }
        catch ( LabelNotFoundKernelException e )
        {
            throw new NotFoundException( format( "Label %s not found", index.getLabel().name() ) );
        }
        catch ( PropertyKeyNotFoundException e )
        {
            throw new NotFoundException( format( "Property key %s not found", propertyKey ) );
        }
        catch ( SchemaRuleNotFoundException | IndexNotFoundKernelException e )
        {
            throw new NotFoundException( format( "No index for label %s on property %s",
                                                 index.getLabel().name(), propertyKey ) );
        }
        finally
        {
            state.close();
        }
    }

    @Override
    public ConstraintCreator constraintFor( Label label )
    {
        assertInTransaction();

        return new BaseConstraintCreator( actions, label );
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints()
    {
        assertInTransaction();

        final StatementOperationParts context = ctxProvider.getCtxForReading();
        StatementState state = ctxProvider.statementForReading();
        try
        {
            Iterator<UniquenessConstraint> constraints = context.schemaReadOperations().constraintsGetAll( state );
            return asConstraintDefinitions( context, state, constraints );
        }
        finally
        {
            state.close();
        }
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints( final Label label )
    {
        assertInTransaction();

        final StatementOperationParts context = ctxProvider.getCtxForReading();
        StatementState state = ctxProvider.statementForReading();
        try
        {
            Iterator<UniquenessConstraint> constraints = context.schemaReadOperations().constraintsGetForLabel(
                    state, context.keyReadOperations().labelGetForName( state, label.name() ) );
            return asConstraintDefinitions( context, state, constraints );
        }
        catch ( LabelNotFoundKernelException e )
        {
            return emptyList();
        }
        finally
        {
            state.close();
        }
    }

    private Iterable<ConstraintDefinition> asConstraintDefinitions( final StatementOperationParts context,
            final StatementState state, Iterator<UniquenessConstraint> constraints )
    {
        Iterator<ConstraintDefinition> definitions =
                map( new Function<UniquenessConstraint, ConstraintDefinition>()
                {
                    @Override
                    public ConstraintDefinition apply( UniquenessConstraint constraint )
                    {
                        long labelId = constraint.label();
                        try
                        {
                            Label label = label( context.keyReadOperations().labelGetName( state, labelId ) );
                            return new PropertyUniqueConstraintDefinition( actions, label,
                                    context.keyReadOperations().propertyKeyGetName( state, constraint.property() ) );
                        }
                        catch ( PropertyKeyIdNotFoundException e )
                        {
                            throw new ThisShouldNotHappenError( "Mattias", "Couldn't find property name for " +
                                                                           constraint.property(), e );
                        }
                        catch ( LabelNotFoundKernelException e )
                        {
                            throw new ThisShouldNotHappenError( "Stefan",
                                                                "Couldn't find label name for label id " +
                                                                labelId, e );
                        }
                    }
                }, constraints );

        // Intentionally iterator over it so that we can close the statement context within this method
        return asCollection( definitions );
    }

    private static class GDBSchemaActions implements InternalSchemaActions
    {

        private final ThreadToStatementContextBridge ctxProvider;
        public GDBSchemaActions( ThreadToStatementContextBridge ctxProvider )
        {
            this.ctxProvider = ctxProvider;
        }

        @Override
        public IndexDefinition createIndexDefinition( Label label, String propertyKey )
        {
            StatementOperationParts context = ctxProvider.getCtxForWriting();
            StatementState state = ctxProvider.statementForWriting();
            try
            {
                long labelId = context.keyWriteOperations().labelGetOrCreateForName( state, label.name() );
                long propertyKeyId = context.keyWriteOperations().propertyKeyGetOrCreateForName( state, propertyKey );
                context.schemaWriteOperations().indexCreate( state, labelId, propertyKeyId );
                return new IndexDefinitionImpl( this, label, propertyKey, false );
            }
            catch ( AlreadyIndexedException e )
            {
                throw new ConstraintViolationException( String.format(
                        "There already exists an index for label '%s' on property '%s'.", label.name(), propertyKey ), e );
            }
            catch ( AlreadyConstrainedException e )
            {
                throw new ConstraintViolationException( String.format(
                        "Label '%s' and property '%s' have a unique constraint defined on them, so an index is " +
                                "already created that matches this.", label.name(), propertyKey ), e );
            }
            catch ( SchemaKernelException e )
            {
                throw new ConstraintViolationException( e.getUserMessage( new KeyNameLookup( state, context.keyReadOperations() ) ), e );
            }
            finally
            {
                state.close();
            }
        }

        @Override
        public void dropIndexDefinitions( Label label, String propertyKey )
        {
            StatementOperationParts context = ctxProvider.getCtxForWriting();
            StatementState state = ctxProvider.statementForWriting();
            try
            {
                long labelId = context.keyReadOperations().labelGetForName( state, label.name() );
                long propertyKeyId = context.keyReadOperations().propertyKeyGetForName( state, propertyKey );
                context.schemaWriteOperations().indexDrop( state,
                        context.schemaReadOperations().indexesGetForLabelAndPropertyKey( state, labelId, propertyKeyId ) );
            }
            catch ( SchemaKernelException e )
            {
                throw new ConstraintViolationException( String.format(
                        "Unable to drop index on label `%s` for property %s.", label.name(), propertyKey ), e );
            }
            catch ( LabelNotFoundKernelException e )
            {
                throw new ThisShouldNotHappenError( "Mattias", "Label " + label.name() + " should exist here" );
            }
            catch ( PropertyKeyNotFoundException e )
            {
                throw new ThisShouldNotHappenError( "Mattias", "Property " + propertyKey + " should exist here" );
            }
            finally
            {
                state.close();
            }
        }

        @Override
        public ConstraintDefinition createPropertyUniquenessConstraint( Label label, String propertyKey )
                throws SchemaKernelException
        {
            StatementOperationParts context = ctxProvider.getCtxForWriting();
            StatementState state = ctxProvider.statementForWriting();
            try
            {
                long labelId = context.keyWriteOperations().labelGetOrCreateForName( state, label.name() );
                long propertyKeyId = context.keyWriteOperations().propertyKeyGetOrCreateForName( state, propertyKey );
                context.schemaWriteOperations().uniquenessConstraintCreate( state, labelId, propertyKeyId );
                return new PropertyUniqueConstraintDefinition( this, label, propertyKey );
            }
            finally
            {
                state.close();
            }
        }

        @Override
        public void dropPropertyUniquenessConstraint( Label label, String propertyKey )
        {
            StatementOperationParts context = ctxProvider.getCtxForWriting();
            StatementState state = ctxProvider.statementForWriting();
            try
            {
                long labelId = context.keyWriteOperations().labelGetOrCreateForName( state, label.name() );
                long propertyKeyId = context.keyWriteOperations().propertyKeyGetOrCreateForName( state, propertyKey );
                UniquenessConstraint constraint = new UniquenessConstraint( labelId, propertyKeyId );
                context.schemaWriteOperations().constraintDrop( state, constraint );
            }
            catch ( SchemaKernelException e )
            {
                throw new ThisShouldNotHappenError( "Mattias", "Unable to drop property unique constraint" );
            }
            finally
            {
                state.close();
            }
        }

        @Override
        public String getUserMessage( KernelException e )
        {
            StatementOperationParts context = ctxProvider.getCtxForWriting();
            StatementState state = ctxProvider.statementForReading();
            try
            {
                return e.getUserMessage( new KeyNameLookup( state, context.keyReadOperations() ) );
            }
            finally
            {
                state.close();
            }
        }

        @Override
        public void assertInTransaction()
        {
            ctxProvider.assertInTransaction();
        }

    }
    private void assertInTransaction()
    {
        ctxProvider.assertInTransaction();
    }
}
