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
package org.neo4j.procedure.builtin;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Predicates;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.helpers.Indexes;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.index.IndexPopulationFailure;
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

    public void awaitIndexByPattern( String indexPattern, long timeout, TimeUnit timeoutUnits )
            throws ProcedureException
    {
        IndexSpecifier specifier = IndexSpecifier.byPattern( indexPattern );
        waitUntilOnline( getIndex( specifier ), specifier, timeout, timeoutUnits );
    }

    public void awaitIndexByName( String indexName, long timeout, TimeUnit timeoutUnits )
            throws ProcedureException
    {
        IndexSpecifier specifier = IndexSpecifier.byName( indexName );
        waitUntilOnline( getIndex( specifier ), specifier, timeout, timeoutUnits );
    }

    public void resampleIndex( String indexSpecification ) throws ProcedureException
    {
        IndexSpecifier specifier = IndexSpecifier.byPattern( indexSpecification );
        try
        {
            triggerSampling( getIndex( specifier ) );
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

    public void awaitIndexResampling( long timeout ) throws ProcedureException
    {
        try
        {
            Indexes.awaitResampling( ktx.schemaRead(), timeout);
        }
        catch ( TimeoutException e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureTimedOut, e, "Index resampling timed out" );
        }
    }

    public Stream<BuiltInProcedures.SchemaIndexInfo> createIndex( String indexSpecification, String providerName ) throws ProcedureException
    {
        return createIndex( indexSpecification, providerName, "index created",
                ( schemaWrite, descriptor, provider ) -> schemaWrite.indexCreate( descriptor, provider, null ) );
    }

    public Stream<BuiltInProcedures.SchemaIndexInfo> createUniquePropertyConstraint( String indexSpecification, String providerName ) throws ProcedureException
    {
        return createIndex( indexSpecification, providerName, "uniqueness constraint online",
                ( schemaWrite, descriptor, provider ) -> schemaWrite.uniquePropertyConstraintCreate( descriptor, provider, null ) );
    }

    public Stream<BuiltInProcedures.SchemaIndexInfo> createNodeKey( String indexSpecification, String providerName ) throws ProcedureException
    {
        return createIndex( indexSpecification, providerName, "node key constraint online",
                ( schemaWrite, descriptor, provider ) -> schemaWrite.nodeKeyConstraintCreate( descriptor, provider, null ) );
    }

    private Stream<BuiltInProcedures.SchemaIndexInfo> createIndex( String indexSpecification, String providerName, String statusMessage,
            IndexCreator indexCreator ) throws ProcedureException
    {
        assertProviderNameNotNull( providerName );
        IndexSpecifier index = IndexSpecifier.byPattern( indexSpecification );
        int labelId = getOrCreateLabelId( index.label() );
        int[] propertyKeyIds = getOrCreatePropertyIds( index.properties() );
        try
        {
            SchemaWrite schemaWrite = ktx.schemaWrite();
            LabelSchemaDescriptor labelSchemaDescriptor = SchemaDescriptor.forLabel( labelId, propertyKeyIds );
            indexCreator.create( schemaWrite, labelSchemaDescriptor, providerName );
            return Stream.of( new BuiltInProcedures.SchemaIndexInfo( indexSpecification, providerName, statusMessage ) );
        }
        catch ( KernelException e )
        {
            throw new ProcedureException( e.status(), e, e.getMessage() );
        }
    }

    private static void assertProviderNameNotNull( String providerName ) throws ProcedureException
    {
        if ( providerName == null )
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, indexProviderNullMessage() );
        }
    }

    private static String indexProviderNullMessage()
    {
        return "Could not create index with specified index provider being null.";
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
        catch ( KernelException e )
        {
            throw new ProcedureException( e.status(), e, e.getMessage() );
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
            catch ( KernelException e )
            {
                throw new ProcedureException( e.status(), e, e.getMessage() );
            }
        }
        return propertyKeyIds;
    }

    private IndexDescriptor getIndex( IndexSpecifier specifier ) throws ProcedureException
    {
        if ( specifier.name() != null )
        {
            // Find index by name.
            IndexDescriptor indexReference = ktx.schemaRead().indexGetForName( specifier.name() );

            if ( indexReference == IndexDescriptor.NO_INDEX )
            {
                throw new ProcedureException( Status.Schema.IndexNotFound, "No such index '%s'", specifier );
            }
            return indexReference;
        }
        else
        {
            // Find index by label and properties.
            int labelId = getLabelId( specifier.label() );
            int[] propertyKeyIds = getPropertyIds( specifier.properties() );
            IndexDescriptor indexReference = ktx.schemaRead().index( labelId, propertyKeyIds );

            if ( indexReference == IndexDescriptor.NO_INDEX )
            {
                throw new ProcedureException( Status.Schema.IndexNotFound, "No such index %s", specifier );
            }
            return indexReference;
        }
    }

    private void waitUntilOnline( IndexDescriptor index, IndexSpecifier indexDescription, long timeout, TimeUnit timeoutUnits ) throws ProcedureException
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

    private boolean isOnline( IndexSpecifier specifier, IndexDescriptor index ) throws ProcedureException
    {
        InternalIndexState state = getState( specifier, index );
        switch ( state )
        {
            case POPULATING:
                return false;
            case ONLINE:
                return true;
            case FAILED:
                String cause = getFailure( specifier, index );
                throw new ProcedureException( Status.Schema.IndexCreationFailed,
                        IndexPopulationFailure.appendCauseOfFailure( "Index %s is in failed state.", cause ), specifier );
            default:
                throw new IllegalStateException( "Unknown index state " + state );
        }
    }

    private InternalIndexState getState( IndexSpecifier specifier, IndexDescriptor index ) throws ProcedureException
    {
        try
        {
            return ktx.schemaRead().indexGetState( index );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.Schema.IndexNotFound, e, "No such index %s", specifier );
        }
    }

    private String getFailure( IndexSpecifier indexDescription, IndexDescriptor index ) throws ProcedureException
    {
        try
        {
            return ktx.schemaRead().indexGetFailure( index );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.Schema.IndexNotFound, e, "No such index %s", indexDescription );
        }
    }

    private void triggerSampling( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        indexingService.triggerIndexSampling( index.schema(), IndexSamplingMode.TRIGGER_REBUILD_ALL );
    }

    @Override
    public void close()
    {
        statement.close();
    }

    @FunctionalInterface
    private interface IndexCreator
    {
        void create( SchemaWrite schemaWrite, LabelSchemaDescriptor descriptor, String providerName ) throws KernelException;
    }
}
