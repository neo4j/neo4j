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
package org.neo4j.kernel.impl.coreapi.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Function;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.InvalidTransactionTypeException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

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
    private final ThreadToStatementContextBridge statementContextSupplier;
    private final InternalSchemaActions actions;

    public SchemaImpl( ThreadToStatementContextBridge statementContextSupplier )
    {
        this.statementContextSupplier = statementContextSupplier;
        this.actions = new GDBSchemaActions( statementContextSupplier );
    }

    @Override
    public IndexCreator indexFor( Label label )
    {
        assertInUnterminatedTransaction();

        return new IndexCreatorImpl( actions, label );
    }

    @Override
    public Iterable<IndexDefinition> getIndexes( final Label label )
    {
        assertInUnterminatedTransaction();

        try ( Statement statement = statementContextSupplier.get() )
        {
            List<IndexDefinition> definitions = new ArrayList<>();
            int labelId = statement.readOperations().labelGetForName( label.name() );
            if ( labelId == KeyReadOperations.NO_SUCH_LABEL )
            {
                return emptyList();
            }
            addDefinitions( definitions, statement.readOperations(), statement.readOperations().indexesGetForLabel( labelId ), false );
            addDefinitions( definitions, statement.readOperations(), statement.readOperations().uniqueIndexesGetForLabel( labelId ), true );
            return definitions;
        }
    }

    @Override
    public Iterable<IndexDefinition> getIndexes()
    {
        assertInUnterminatedTransaction();

        try ( Statement statement = statementContextSupplier.get() )
        {
            List<IndexDefinition> definitions = new ArrayList<>();
            addDefinitions( definitions, statement.readOperations(), statement.readOperations().indexesGetAll(), false );
            addDefinitions( definitions, statement.readOperations(), statement.readOperations().uniqueIndexesGetAll(), true );
            return definitions;
        }
    }

    private void addDefinitions( List<IndexDefinition> definitions, final ReadOperations statement,
                                 Iterator<IndexDescriptor> indexes, final boolean constraintIndex )
    {
        addToCollection( map( new Function<IndexDescriptor,IndexDefinition>()
        {
            @Override
            public IndexDefinition apply( IndexDescriptor rule )
            {
                try
                {
                    Label label = label( statement.labelGetName( rule.getLabelId() ) );
                    String propertyKey = statement.propertyKeyGetName( rule.getPropertyKeyId() );
                    return new IndexDefinitionImpl( actions, label, propertyKey, constraintIndex );
                }
                catch ( LabelNotFoundKernelException | PropertyKeyIdNotFoundKernelException e )
                {
                    throw new RuntimeException( e );
                }
            }
        }, indexes ), definitions );
    }

    @Override
    public void awaitIndexOnline( IndexDefinition index, long duration, TimeUnit unit )
    {
        assertInUnterminatedTransaction();

        long timeout = System.currentTimeMillis() + unit.toMillis( duration );
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
        assertInUnterminatedTransaction();

        long millisLeft = TimeUnit.MILLISECONDS.convert( duration, unit );
        Collection<IndexDefinition> onlineIndexes = new ArrayList<>();

        for ( Iterator<IndexDefinition> iter = getIndexes().iterator(); iter.hasNext(); )
        {
            if ( millisLeft < 0 )
            {
                throw new IllegalStateException( "Expected all indexes to come online within a reasonable time."
                                                 + "Indexes brought online: " + onlineIndexes
                                                 + ". Indexes not guaranteed to be online: " + asCollection( iter ) );
            }

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
        assertInUnterminatedTransaction();

        String propertyKey = single( index.getPropertyKeys() );
        try ( Statement statement = statementContextSupplier.get() )
        {
            int labelId = statement.readOperations().labelGetForName( index.getLabel().name() );
            int propertyKeyId = statement.readOperations().propertyKeyGetForName( propertyKey );

            if ( labelId == KeyReadOperations.NO_SUCH_LABEL )
            {
                throw new NotFoundException( format( "Label %s not found", index.getLabel().name() ) );
            }

            if ( propertyKeyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
            {
                throw new NotFoundException( format( "Property key %s not found", propertyKey ) );
            }

            IndexDescriptor descriptor = statement.readOperations().indexesGetForLabelAndPropertyKey( labelId, propertyKeyId );
            InternalIndexState indexState = statement.readOperations().indexGetState( descriptor );
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
        catch ( SchemaRuleNotFoundException | IndexNotFoundKernelException e )
        {
            throw new NotFoundException( format( "No index for label %s on property %s",
                    index.getLabel().name(), propertyKey ) );
        }
    }

    @Override
    public String getIndexFailure( IndexDefinition index )
    {
        assertInUnterminatedTransaction();

        String propertyKey = single( index.getPropertyKeys() );
        try ( Statement statement = statementContextSupplier.get() )
        {
            int labelId = statement.readOperations().labelGetForName( index.getLabel().name() );
            int propertyKeyId = statement.readOperations().propertyKeyGetForName( propertyKey );

            if ( labelId == KeyReadOperations.NO_SUCH_LABEL )
            {
                throw new NotFoundException( format( "Label %s not found", index.getLabel().name() ) );
            }

            if ( propertyKeyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
            {
                throw new NotFoundException( format( "Property key %s not found", propertyKey ) );
            }

            IndexDescriptor indexId = statement.readOperations().indexesGetForLabelAndPropertyKey( labelId, propertyKeyId );
            return statement.readOperations().indexGetFailure( indexId );
        }
        catch ( SchemaRuleNotFoundException | IndexNotFoundKernelException e )
        {
            throw new NotFoundException( format( "No index for label %s on property %s",
                    index.getLabel().name(), propertyKey ) );
        }
    }

    @Override
    public ConstraintCreator constraintFor( Label label )
    {
        assertInUnterminatedTransaction();

        return new BaseNodeConstraintCreator( actions, label );
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints()
    {
        assertInUnterminatedTransaction();

        try ( Statement statement = statementContextSupplier.get() )
        {
            Iterator<PropertyConstraint> constraints = statement.readOperations().constraintsGetAll();
            return asConstraintDefinitions( constraints, statement.readOperations() );
        }
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints( final Label label )
    {
        assertInUnterminatedTransaction();

        try ( Statement statement = statementContextSupplier.get() )
        {
            int labelId = statement.readOperations().labelGetForName( label.name() );
            if ( labelId == KeyReadOperations.NO_SUCH_LABEL )
            {
                return emptyList();
            }
            Iterator<NodePropertyConstraint> constraints = statement.readOperations().constraintsGetForLabel( labelId );
            return asConstraintDefinitions( constraints, statement.readOperations() );
        }
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints( RelationshipType type )
    {
        assertInUnterminatedTransaction();

        try ( Statement statement = statementContextSupplier.get() )
        {
            int typeId = statement.readOperations().relationshipTypeGetForName( type.name() );
            if ( typeId == KeyReadOperations.NO_SUCH_RELATIONSHIP_TYPE )
            {
                return emptyList();
            }
            Iterator<RelationshipPropertyConstraint> constraints =
                    statement.readOperations().constraintsGetForRelationshipType( typeId );
            return asConstraintDefinitions( constraints, statement.readOperations() );
        }
    }

    private Iterable<ConstraintDefinition> asConstraintDefinitions( Iterator<? extends PropertyConstraint> constraints,
            ReadOperations readOperations )
    {
        // Intentionally create an eager list so that used statement can be closed
        List<ConstraintDefinition> definitions = new ArrayList<>();

        while ( constraints.hasNext() )
        {
            PropertyConstraint constraint = constraints.next();
            definitions.add( constraint.asConstraintDefinition( actions, readOperations ) );
        }

        return definitions;
    }

    private static class GDBSchemaActions implements InternalSchemaActions
    {
        private final ThreadToStatementContextBridge ctxSupplier;
        public GDBSchemaActions( ThreadToStatementContextBridge ctxSupplier )
        {
            this.ctxSupplier = ctxSupplier;
        }

        @Override
        public IndexDefinition createIndexDefinition( Label label, String propertyKey )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.schemaWriteOperations().labelGetOrCreateForName( label.name() );
                    int propertyKeyId = statement.schemaWriteOperations().propertyKeyGetOrCreateForName( propertyKey );
                    statement.schemaWriteOperations().indexCreate( labelId, propertyKeyId );
                    return new IndexDefinitionImpl( this, label, propertyKey, false );
                }
                catch ( AlreadyIndexedException | AlreadyConstrainedException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( IllegalTokenNameException e )
                {
                    throw new IllegalArgumentException( e );
                }
                catch ( TooManyLabelsException e )
                {
                    throw new IllegalStateException( e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public void dropIndexDefinitions( Label label, String propertyKey )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.readOperations().labelGetForName( label.name() );
                    int propertyKeyId = statement.readOperations().propertyKeyGetForName( propertyKey );

                    if ( labelId != KeyReadOperations.NO_SUCH_LABEL && propertyKeyId != KeyReadOperations
                            .NO_SUCH_PROPERTY_KEY )
                    {
                        statement.schemaWriteOperations().indexDrop(
                                statement.readOperations().indexesGetForLabelAndPropertyKey( labelId, propertyKeyId ) );
                    }
                }
                catch ( SchemaRuleNotFoundException | DropIndexFailureException e )
                {
                    throw new ConstraintViolationException( e.getUserMessage(
                            new StatementTokenNameLookup( statement.readOperations() ) ) );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public ConstraintDefinition createPropertyUniquenessConstraint( Label label, String propertyKey )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.schemaWriteOperations().labelGetOrCreateForName( label.name() );
                    int propertyKeyId = statement.schemaWriteOperations().propertyKeyGetOrCreateForName( propertyKey );
                    statement.schemaWriteOperations().uniquePropertyConstraintCreate( labelId, propertyKeyId );
                    return new UniquenessConstraintDefinition( this, label, propertyKey );
                }
                catch ( AlreadyConstrainedException | CreateConstraintFailureException | AlreadyIndexedException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( IllegalTokenNameException e )
                {
                    throw new IllegalArgumentException( e );
                }
                catch ( TooManyLabelsException e )
                {
                    throw new IllegalStateException( e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new InvalidTransactionTypeException( e.getMessage(), e );
                }
            }
        }

        @Override
        public ConstraintDefinition createPropertyExistenceConstraint( Label label, String propertyKey )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.schemaWriteOperations().labelGetOrCreateForName( label.name() );
                    int propertyKeyId = statement.schemaWriteOperations().propertyKeyGetOrCreateForName( propertyKey );
                    statement.schemaWriteOperations().nodePropertyExistenceConstraintCreate( labelId, propertyKeyId );
                    return new NodePropertyExistenceConstraintDefinition( this, label, propertyKey );
                }
                catch ( AlreadyConstrainedException | CreateConstraintFailureException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( IllegalTokenNameException e )
                {
                    throw new IllegalArgumentException( e );
                }
                catch ( TooManyLabelsException e )
                {
                    throw new IllegalStateException( e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new InvalidTransactionTypeException( e.getMessage(), e );
                }
            }
        }

        @Override
        public ConstraintDefinition createPropertyExistenceConstraint( RelationshipType type, String propertyKey )
                throws CreateConstraintFailureException, AlreadyConstrainedException
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int typeId = statement.schemaWriteOperations().relationshipTypeGetOrCreateForName( type.name() );
                    int propertyKeyId = statement.schemaWriteOperations().propertyKeyGetOrCreateForName( propertyKey );
                    statement.schemaWriteOperations().relationshipPropertyExistenceConstraintCreate( typeId,
                            propertyKeyId );
                    return new RelationshipPropertyExistenceConstraintDefinition( this, type, propertyKey );
                }
                catch ( AlreadyConstrainedException | CreateConstraintFailureException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( IllegalTokenNameException e )
                {
                    throw new IllegalArgumentException( e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new InvalidTransactionTypeException( e.getMessage(), e );
                }
            }
        }

        @Override
        public void dropPropertyUniquenessConstraint( Label label, String propertyKey )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.schemaWriteOperations().labelGetForName( label.name() );
                    int propertyKeyId = statement.schemaWriteOperations().propertyKeyGetForName( propertyKey );
                    NodePropertyConstraint constraint = new UniquenessConstraint( labelId, propertyKeyId );
                    statement.schemaWriteOperations().constraintDrop( constraint );
                }
                catch ( DropConstraintFailureException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public void dropNodePropertyExistenceConstraint( Label label, String propertyKey )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.schemaWriteOperations().labelGetForName( label.name() );
                    int propertyKeyId = statement.schemaWriteOperations().propertyKeyGetForName( propertyKey );
                    NodePropertyConstraint constraint = new NodePropertyExistenceConstraint( labelId, propertyKeyId );
                    statement.schemaWriteOperations().constraintDrop( constraint );
                }
                catch ( DropConstraintFailureException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public void dropRelationshipPropertyExistenceConstraint( RelationshipType type, String propertyKey )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int typeId = statement.schemaWriteOperations().relationshipTypeGetForName( type.name() );
                    int propertyKeyId = statement.schemaWriteOperations().propertyKeyGetForName( propertyKey );
                    RelationshipPropertyConstraint constraint = new RelationshipPropertyExistenceConstraint( typeId,
                            propertyKeyId );
                    statement.schemaWriteOperations().constraintDrop( constraint );
                }
                catch ( DropConstraintFailureException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public String getUserMessage( KernelException e )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                return e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) );
            }
        }

        @Override
        public void assertInUnterminatedTransaction()
        {
            ctxSupplier.assertInUnterminatedTransaction();
        }
    }

    private void assertInUnterminatedTransaction()
    {
        statementContextSupplier.assertInUnterminatedTransaction();
    }
}
