/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.builtinprocs;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.neo4j.function.Predicates;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;

public class IndexProcedures implements AutoCloseable
{
    private final KernelTransaction ktx;
    private final Statement statement;
    private final IndexingService indexingService;

    public IndexProcedures( KernelTransaction tx, IndexingService indexingService )
    {
        this.ktx = tx;
        statement = tx.acquireStatement();
        this.indexingService = indexingService;
    }

    public void awaitIndex( String indexSpecification, long timeout, TimeUnit timeoutUnits )
            throws ProcedureException
    {
        IndexSpecifier index = parse( indexSpecification );
        int labelId = getLabelId( index.label() );
        int[] propertyKeyIds = getPropertyIds( index.properties() );
        waitUntilOnline( getIndex( labelId, propertyKeyIds, index ), index, timeout, timeoutUnits );
    }

    public void resampleIndex( String indexSpecification ) throws ProcedureException
    {
        IndexSpecifier index = parse( indexSpecification );
        int labelId = getLabelId( index.label() );
        int[] propertyKeyIds = getPropertyIds( index.properties() );
        try
        {
            triggerSampling( getIndex( labelId, propertyKeyIds, index ) );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new ProcedureException( e.status(), e.getMessage(), e );
        }
    }

    public void resampleOutdatedIndexes()
    {
        indexingService.triggerIndexSampling( IndexSamplingMode.TRIGGER_REBUILD_UPDATED );
    }

    public Stream<BuiltInProcedures.SchemaIndexInfo> createIndex( String indexSpecification, String providerName ) throws ProcedureException
    {
        String statusMessage = "index created";
        return createIndex( indexSpecification, providerName, statusMessage, SchemaWrite::indexCreate );
    }

    public Stream<BuiltInProcedures.SchemaIndexInfo> createUniquePropertyConstraint( String indexSpecification, String providerName ) throws ProcedureException
    {
        return createIndex( indexSpecification, providerName, "uniqueness constraint online", SchemaWrite::uniquePropertyConstraintCreate );
    }

    public Stream<BuiltInProcedures.SchemaIndexInfo> createNodeKey( String indexSpecification, String providerName ) throws ProcedureException
    {
        return createIndex( indexSpecification, providerName, "node key constraint online", SchemaWrite::nodeKeyConstraintCreate );
    }

    private Stream<BuiltInProcedures.SchemaIndexInfo> createIndex( String indexSpecification, String providerName, String statusMessage,
            IndexCreator indexCreator ) throws ProcedureException
    {
        assertProviderNameNotNull( providerName );
        IndexSpecifier index = parse( indexSpecification );
        int labelId = getOrCreateLabelId( index.label() );
        int[] propertyKeyIds = getOrCreatePropertyIds( index.properties() );
        try
        {
            SchemaWrite schemaWrite = ktx.schemaWrite();
            LabelSchemaDescriptor labelSchemaDescriptor = SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds );
            indexCreator.create( schemaWrite, labelSchemaDescriptor, providerName );
            return Stream.of( new BuiltInProcedures.SchemaIndexInfo( indexSpecification, providerName, statusMessage ) );
        }
        catch ( InvalidTransactionTypeKernelException | SchemaKernelException e )
        {
            throw new ProcedureException( e.status(), e, e.getMessage() );
        }
    }

    private void assertProviderNameNotNull( String providerName ) throws ProcedureException
    {
        if ( providerName == null )
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, indexProviderNullMessage() );
        }
    }

    private String indexProviderNullMessage()
    {
        return "Could not create index with specified index provider being null.";
    }

    private IndexSpecifier parse( String specification )
    {
        return new IndexSpecifier( specification );
    }

    private int getLabelId( String labelName ) throws ProcedureException
    {
        int labelId = ktx.tokenRead().nodeLabel( labelName );
        if ( labelId == TokenRead.NO_TOKEN )
        {
            throw new ProcedureException( Status.Schema.LabelAccessFailed, "No such label %s", labelName );
        }
        return labelId;
    }

    private int[] getPropertyIds( String[] propertyKeyNames ) throws ProcedureException
    {
        int[] propertyKeyIds = new int[propertyKeyNames.length];
        for ( int i = 0; i < propertyKeyIds.length; i++ )
        {

            int propertyKeyId = ktx.tokenRead().propertyKey( propertyKeyNames[i] );
            if ( propertyKeyId == TokenRead.NO_TOKEN )
            {
                throw new ProcedureException( Status.Schema.PropertyKeyAccessFailed, "No such property key %s", propertyKeyNames[i] );
            }
            propertyKeyIds[i] = propertyKeyId;
        }
        return propertyKeyIds;
    }

    private int getOrCreateLabelId( String labelName ) throws ProcedureException
    {
        try
        {
            return ktx.tokenWrite().labelGetOrCreateForName( labelName );
        }
        catch ( TooManyLabelsException | IllegalTokenNameException e )
        {
            throw new ProcedureException( e.status(), e, "" );
        }
    }

    private int[] getOrCreatePropertyIds( String[] propertyKeyNames ) throws ProcedureException
    {
        int[] propertyKeyIds = new int[propertyKeyNames.length];
        for ( int i = 0; i < propertyKeyIds.length; i++ )
        {
            try
            {
                propertyKeyIds[i] = ktx.tokenWrite().propertyKeyGetOrCreateForName( propertyKeyNames[i] );
            }
            catch ( IllegalTokenNameException e )
            {
                throw new ProcedureException( e.status(), e, "" );
            }
        }
        return propertyKeyIds;
    }

    private CapableIndexReference getIndex( int labelId, int[] propertyKeyIds, IndexSpecifier index ) throws
            ProcedureException
    {
        CapableIndexReference indexReference = ktx.schemaRead().index( labelId, propertyKeyIds );

        if ( indexReference == CapableIndexReference.NO_INDEX )
        {
            throw new ProcedureException( Status.Schema.IndexNotFound, "No index on %s", index );
        }
        return indexReference;
    }

    private void waitUntilOnline( IndexReference index, IndexSpecifier indexDescription,
                                  long timeout, TimeUnit timeoutUnits )
            throws ProcedureException
    {
        try
        {
            Predicates.awaitEx( () -> isOnline( indexDescription, index ), timeout, timeoutUnits );
        }
        catch ( TimeoutException e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureTimedOut,
                    "Index on %s did not come online within %s %s", indexDescription, timeout, timeoutUnits );
        }
    }

    private boolean isOnline( IndexSpecifier indexDescription, IndexReference index ) throws ProcedureException
    {
        InternalIndexState state = getState( indexDescription, index );
        switch ( state )
        {
            case POPULATING:
                return false;
            case ONLINE:
                return true;
            case FAILED:
                throw new ProcedureException( Status.Schema.IndexCreationFailed,
                        "Index on %s is in failed state", indexDescription );
            default:
                throw new IllegalStateException( "Unknown index state " + state );
        }
    }

    private InternalIndexState getState( IndexSpecifier indexDescription, IndexReference index )
            throws ProcedureException
    {
        try
        {
            return ktx.schemaRead().indexGetState( index );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.Schema.IndexNotFound, e, "No index on %s", indexDescription );
        }
    }

    private void triggerSampling( IndexReference index ) throws IndexNotFoundKernelException
    {
        indexingService.triggerIndexSampling( SchemaDescriptorFactory.forLabel( index.label(), index.properties() ), IndexSamplingMode.TRIGGER_REBUILD_ALL );
    }

    @Override
    public void close()
    {
        statement.close();
    }

    @FunctionalInterface
    private interface IndexCreator
    {
        void create( SchemaWrite schemaWrite, LabelSchemaDescriptor descriptor, String providerName ) throws SchemaKernelException;
    }
}
