/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.InvalidTransactionTypeException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexPopulationProgress;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.TokenWriteOperations;
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
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInCompositeSchemaException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.NodeExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.storageengine.api.schema.PopulationProgress;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.schema.Schema.IndexState.FAILED;
import static org.neo4j.graphdb.schema.Schema.IndexState.ONLINE;
import static org.neo4j.graphdb.schema.Schema.IndexState.POPULATING;
import static org.neo4j.helpers.collection.Iterators.addToCollection;
import static org.neo4j.helpers.collection.Iterators.asCollection;
import static org.neo4j.helpers.collection.Iterators.map;
import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Type.UNIQUE;
import static org.neo4j.kernel.impl.coreapi.schema.PropertyNameUtils.getOrCreatePropertyKeyIds;

public class SchemaImpl implements Schema
{
    private final Supplier<Statement> statementContextSupplier;
    private final InternalSchemaActions actions;

    public SchemaImpl( Supplier<Statement> statementSupplier )
    {
        this.statementContextSupplier = statementSupplier;
        this.actions = new GDBSchemaActions( statementSupplier );
    }

    @Override
    public IndexCreator indexFor( Label label )
    {
        return new IndexCreatorImpl( actions, label );
    }

    @Override
    public Iterable<IndexDefinition> getIndexes( final Label label )
    {
        try ( Statement statement = statementContextSupplier.get() )
        {
            List<IndexDefinition> definitions = new ArrayList<>();
            int labelId = statement.readOperations().labelGetForName( label.name() );
            if ( labelId == KeyReadOperations.NO_SUCH_LABEL )
            {
                return emptyList();
            }
            Iterator<IndexDescriptor> indexes = statement.readOperations().indexesGetForLabel( labelId );
            addDefinitions( definitions, statement.readOperations(), IndexDescriptor.sortByType( indexes ) );
            return definitions;
        }
    }

    @Override
    public Iterable<IndexDefinition> getIndexes()
    {
        try ( Statement statement = statementContextSupplier.get() )
        {
            List<IndexDefinition> definitions = new ArrayList<>();
            Iterator<IndexDescriptor> indexes = statement.readOperations().indexesGetAll();
            addDefinitions( definitions, statement.readOperations(), IndexDescriptor.sortByType( indexes ) );
            return definitions;
        }
    }

    private IndexDefinition descriptorToDefinition( final ReadOperations statement, IndexDescriptor index )
    {
        try
        {
            Label label = label( statement.labelGetName( index.schema().getLabelId() ) );
            boolean constraintIndex = index.type() == UNIQUE;
            String[] propertyNames = PropertyNameUtils.getPropertyKeys( statement, index.schema().getPropertyIds() );
            return new IndexDefinitionImpl( actions, label, propertyNames, constraintIndex );
        }
        catch ( LabelNotFoundKernelException | PropertyKeyIdNotFoundKernelException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void addDefinitions( List<IndexDefinition> definitions, final ReadOperations statement,
                                 Iterator<IndexDescriptor> indexes )
    {
        addToCollection(
                map( index -> descriptorToDefinition( statement, index ), indexes ),
                definitions );
    }

    @Override
    public void awaitIndexOnline( IndexDefinition index, long duration, TimeUnit unit )
    {
        actions.assertInOpenTransaction();
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
        actions.assertInOpenTransaction();
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
        actions.assertInOpenTransaction();
        try ( Statement statement = statementContextSupplier.get() )
        {
            ReadOperations readOps = statement.readOperations();
            IndexDescriptor descriptor = getIndexDescriptor( readOps, index );
            InternalIndexState indexState = readOps.indexGetState( descriptor );
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
                    index.getLabel().name(), index.getPropertyKeys() ) );
        }
    }

    @Override
    public IndexPopulationProgress getIndexPopulationProgress( IndexDefinition index )
    {
        actions.assertInOpenTransaction();
        try ( Statement statement = statementContextSupplier.get() )
        {
            ReadOperations readOps = statement.readOperations();
            IndexDescriptor descriptor = getIndexDescriptor( readOps, index );
            PopulationProgress progress = readOps.indexGetPopulationProgress( descriptor );
            return new IndexPopulationProgress( progress.getCompleted(), progress.getTotal() );
        }
        catch ( SchemaRuleNotFoundException | IndexNotFoundKernelException e )
        {
            throw new NotFoundException( format( "No index for label %s on property %s", index.getLabel().name(),
                    index.getPropertyKeys() ) );
        }
    }

    @Override
    public String getIndexFailure( IndexDefinition index )
    {
        actions.assertInOpenTransaction();
        try ( Statement statement = statementContextSupplier.get() )
        {
            ReadOperations readOps = statement.readOperations();
            IndexDescriptor descriptor = getIndexDescriptor( readOps, index );
            return readOps.indexGetFailure( descriptor );
        }
        catch ( SchemaRuleNotFoundException | IndexNotFoundKernelException e )
        {
            throw new NotFoundException( format( "No index for label %s on property %s",
                    index.getLabel().name(), index.getPropertyKeys() ) );
        }
    }

    @Override
    public ConstraintCreator constraintFor( Label label )
    {
        actions.assertInOpenTransaction();
        return new BaseNodeConstraintCreator( actions, label );
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints()
    {
        actions.assertInOpenTransaction();
        try ( Statement statement = statementContextSupplier.get() )
        {
            Iterator<ConstraintDescriptor> constraints = statement.readOperations().constraintsGetAll();
            return asConstraintDefinitions( constraints, statement.readOperations() );
        }
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints( final Label label )
    {
        actions.assertInOpenTransaction();

        try ( Statement statement = statementContextSupplier.get() )
        {
            int labelId = statement.readOperations().labelGetForName( label.name() );
            if ( labelId == KeyReadOperations.NO_SUCH_LABEL )
            {
                return emptyList();
            }
            Iterator<ConstraintDescriptor> constraints = statement.readOperations().constraintsGetForLabel( labelId );
            return asConstraintDefinitions( constraints, statement.readOperations() );
        }
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints( RelationshipType type )
    {
        actions.assertInOpenTransaction();
        try ( Statement statement = statementContextSupplier.get() )
        {
            int typeId = statement.readOperations().relationshipTypeGetForName( type.name() );
            if ( typeId == KeyReadOperations.NO_SUCH_RELATIONSHIP_TYPE )
            {
                return emptyList();
            }
            Iterator<ConstraintDescriptor> constraints =
                    statement.readOperations().constraintsGetForRelationshipType( typeId );
            return asConstraintDefinitions( constraints, statement.readOperations() );
        }
    }

    private static IndexDescriptor getIndexDescriptor( ReadOperations readOperations, IndexDefinition index )
            throws SchemaRuleNotFoundException
    {
        int labelId = readOperations.labelGetForName( index.getLabel().name() );
        int[] propertyKeyIds = PropertyNameUtils.getPropertyIds( readOperations, index.getPropertyKeys() );
        assertValidLabel( index.getLabel(), labelId );
        assertValidProperties( index.getPropertyKeys(), propertyKeyIds );
        return readOperations.indexGetForSchema( SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds ) );
    }

    private static void assertValidLabel( Label label, int labelId )
    {
        if ( labelId == KeyReadOperations.NO_SUCH_LABEL )
        {
            throw new NotFoundException( format( "Label %s not found", label.name() ) );
        }
    }

    private static void assertValidProperties( Iterable<String> properties , int[] propertyIds )
    {
        for ( int i = 0; i < propertyIds.length; i++ )
        {
            if ( propertyIds[i] == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
            {
                throw new NotFoundException(
                        format( "Property key %s not found", Iterables.asArray( String.class, properties )[i] ) );
            }
        }
    }

    private Iterable<ConstraintDefinition> asConstraintDefinitions( Iterator<? extends ConstraintDescriptor> constraints,
            ReadOperations readOperations )
    {
        // Intentionally create an eager list so that used statement can be closed
        List<ConstraintDefinition> definitions = new ArrayList<>();

        while ( constraints.hasNext() )
        {
            ConstraintDescriptor constraint = constraints.next();
            definitions.add( asConstraintDefinition( constraint, readOperations ) );
        }

        return definitions;
    }

    private ConstraintDefinition asConstraintDefinition( ConstraintDescriptor constraint,
            ReadOperations readOperations )
    {
        // This was turned inside out. Previously a low-level constraint object would reference a public enum type
        // which made it impossible to break out the low-level component from kernel. There could be a lower level
        // constraint type introduced to mimic the public ConstraintType, but that would be a duplicate of it
        // essentially. Checking instanceof here is OKish since the objects it checks here are part of the
        // internal storage engine API.
        StatementTokenNameLookup lookup = new StatementTokenNameLookup( readOperations );
        if ( constraint instanceof NodeExistenceConstraintDescriptor ||
             constraint instanceof NodeKeyConstraintDescriptor ||
             constraint instanceof UniquenessConstraintDescriptor )
        {
            LabelSchemaDescriptor schemaDescriptor = (LabelSchemaDescriptor) constraint.schema();
            Label label = Label.label( lookup.labelGetName( schemaDescriptor.getLabelId() ) );
            String[] propertyKeys = Arrays.stream( schemaDescriptor.getPropertyIds() )
                    .mapToObj( lookup::propertyKeyGetName ).toArray(String[]::new);
            if ( constraint instanceof NodeExistenceConstraintDescriptor )
            {
                return new NodePropertyExistenceConstraintDefinition( actions, label, propertyKeys );
            }
            else if ( constraint instanceof UniquenessConstraintDescriptor )
            {
                return new UniquenessConstraintDefinition( actions, new IndexDefinitionImpl( actions, label,
                        propertyKeys, true ) );
            }
            else if ( constraint instanceof NodeKeyConstraintDescriptor )
            {
                return new NodeKeyConstraintDefinition( actions, new IndexDefinitionImpl( actions, label,
                        propertyKeys, true ) );
            }
        }
        else if ( constraint instanceof RelExistenceConstraintDescriptor )
        {
            RelationTypeSchemaDescriptor descriptor = (RelationTypeSchemaDescriptor) constraint.schema();
            return new RelationshipPropertyExistenceConstraintDefinition( actions,
                    RelationshipType.withName( lookup.relationshipTypeGetName( descriptor.getRelTypeId() ) ),
                    lookup.propertyKeyGetName( descriptor.getPropertyId() ) );
        }
        throw new IllegalArgumentException( "Unknown constraint " + constraint );
    }

    private static class GDBSchemaActions implements InternalSchemaActions
    {
        private final Supplier<Statement> ctxSupplier;

        GDBSchemaActions( Supplier<Statement> statementSupplier )
        {
            this.ctxSupplier = statementSupplier;
        }

        @Override
        public IndexDefinition createIndexDefinition( Label label, String... propertyKeys )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    IndexDefinition indexDefinition = new IndexDefinitionImpl( this, label, propertyKeys, false );
                    TokenWriteOperations tokenWriteOperations = statement.tokenWriteOperations();
                    int labelId = tokenWriteOperations.labelGetOrCreateForName( indexDefinition.getLabel().name() );
                    int[] propertyKeyIds = getOrCreatePropertyKeyIds( tokenWriteOperations, indexDefinition );
                    LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds );
                    statement.schemaWriteOperations().indexCreate( descriptor );
                    return indexDefinition;
                }
                catch ( AlreadyIndexedException | AlreadyConstrainedException | RepeatedPropertyInCompositeSchemaException e )
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
        public void dropIndexDefinitions( IndexDefinition indexDefinition )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    statement.schemaWriteOperations().indexDrop(
                            getIndexDescriptor( statement.readOperations(), indexDefinition ) );
                }
                catch ( NotFoundException e )
                {
                    // Silently ignore invalid label and property names
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
        public ConstraintDefinition createPropertyUniquenessConstraint( IndexDefinition indexDefinition )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.tokenWriteOperations().labelGetOrCreateForName(
                            indexDefinition.getLabel().name() );
                    int[] propertyKeyIds = getOrCreatePropertyKeyIds(
                            statement.tokenWriteOperations(), indexDefinition );
                    statement.schemaWriteOperations().uniquePropertyConstraintCreate(
                            SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds ) );
                    return new UniquenessConstraintDefinition( this, indexDefinition );
                }
                catch ( AlreadyConstrainedException | CreateConstraintFailureException | AlreadyIndexedException |
                        RepeatedPropertyInCompositeSchemaException e )
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
        public ConstraintDefinition createNodeKeyConstraint( IndexDefinition indexDefinition )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.tokenWriteOperations().labelGetOrCreateForName(
                            indexDefinition.getLabel().name() );
                    int[] propertyKeyIds = getOrCreatePropertyKeyIds(
                            statement.tokenWriteOperations(), indexDefinition );
                    statement.schemaWriteOperations().nodeKeyConstraintCreate(
                            SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds ) );
                    return new NodeKeyConstraintDefinition( this, indexDefinition );
                }
                catch ( AlreadyConstrainedException | CreateConstraintFailureException | AlreadyIndexedException |
                        RepeatedPropertyInCompositeSchemaException e )
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
        public ConstraintDefinition createPropertyExistenceConstraint( Label label, String... propertyKeys )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( label.name() );
                    int[] propertyKeyIds = getOrCreatePropertyKeyIds(
                            statement.tokenWriteOperations(), propertyKeys );
                    statement.schemaWriteOperations().nodePropertyExistenceConstraintCreate(
                                    SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds ) );
                    return new NodePropertyExistenceConstraintDefinition( this, label, propertyKeys );
                }
                catch ( AlreadyConstrainedException | CreateConstraintFailureException |
                        RepeatedPropertyInCompositeSchemaException e )
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
                    int typeId = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( type.name() );
                    int[] propertyKeyId = getOrCreatePropertyKeyIds( statement.tokenWriteOperations(), propertyKey );
                    statement.schemaWriteOperations().relationshipPropertyExistenceConstraintCreate(
                            SchemaDescriptorFactory.forRelType( typeId, propertyKeyId ) );
                    return new RelationshipPropertyExistenceConstraintDefinition( this, type, propertyKey );
                }
                catch ( AlreadyConstrainedException | CreateConstraintFailureException |
                        RepeatedPropertyInCompositeSchemaException e )
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
        public void dropPropertyUniquenessConstraint( Label label, String[] properties )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.readOperations().labelGetForName( label.name() );
                    int[] propertyKeyIds = PropertyNameUtils.getPropertyIds( statement.readOperations(), properties );
                    statement.schemaWriteOperations().constraintDrop(
                            ConstraintDescriptorFactory.uniqueForLabel( labelId, propertyKeyIds ) );
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
        public void dropNodeKeyConstraint( Label label, String[] properties )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.readOperations().labelGetForName( label.name() );
                    int[] propertyKeyIds = PropertyNameUtils.getPropertyIds( statement.readOperations(), properties );
                    statement.schemaWriteOperations().constraintDrop(
                            ConstraintDescriptorFactory.nodeKeyForLabel( labelId, propertyKeyIds ) );
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
        public void dropNodePropertyExistenceConstraint( Label label, String[] properties )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.readOperations().labelGetForName( label.name() );
                    int[] propertyKeyIds = PropertyNameUtils.getPropertyIds( statement.readOperations(), properties );
                    statement.schemaWriteOperations().constraintDrop(
                            ConstraintDescriptorFactory.existsForLabel( labelId, propertyKeyIds ) );
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
                    int typeId = statement.readOperations().relationshipTypeGetForName( type.name() );
                    int propertyKeyId = statement.readOperations().propertyKeyGetForName( propertyKey );
                    statement.schemaWriteOperations().constraintDrop(
                            ConstraintDescriptorFactory.existsForRelType( typeId, propertyKeyId ) );
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
        public void assertInOpenTransaction()
        {
            ctxSupplier.get().close();
        }
    }
}
