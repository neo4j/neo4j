/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext.integrations.kernel;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.impl.fulltext.lucene.FulltextFactory;
import org.neo4j.kernel.api.impl.fulltext.lucene.FulltextProviderImpl;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.kernel.api.impl.fulltext.lucene.FulltextFactory.getType;

public class FulltextIndexProvider extends IndexProvider<FulltextIndexDescriptor>
{
    private final FulltextFactory factory;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final FulltextProviderImpl oldProvider;

    protected FulltextIndexProvider( Descriptor descriptor, int priority, IndexDirectoryStructure.Factory directoryStructureFactory,
            FileSystemAbstraction fileSystem, String analyzerClassName, PropertyKeyTokenHolder propertyKeyTokenHolder, LogService logging,
            AvailabilityGuard availabilityGuard, GraphDatabaseService db, JobScheduler scheduler, Supplier<TransactionIdStore> transactionIdStore,
            File storeDir ) throws IOException
    {
        super( descriptor, priority, directoryStructureFactory );
        Log log = logging.getInternalLog( FulltextProviderImpl.class );
        this.oldProvider = new FulltextProviderImpl( db, log, availabilityGuard, scheduler, transactionIdStore, fileSystem, storeDir, analyzerClassName );
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        factory = new FulltextFactory( fileSystem, directoryStructure().rootDirectory(), analyzerClassName );
    }

    @Override
    public FulltextIndexDescriptor indexDescriptorFor( SchemaDescriptor schema, String name )
    {
        try
        {
            return new FulltextIndexDescriptor( schema, name, propertyKeyTokenHolder );
        }
        catch ( TokenNotFoundException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public IndexPopulator getPopulator( long indexId, FulltextIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        //factory.createFulltextIndex( descriptor );
        return new FulltextIndexPopulator(indexId, descriptor, samplingConfig, oldProvider);
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, FulltextIndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        throw new IllegalStateException( "Not implemented" );
    }

    @Override
    public String getPopulationFailure( long indexId ) throws IllegalStateException
    {
        return "TODO Failure";
    }

    @Override
    public InternalIndexState getInitialState( long indexId, FulltextIndexDescriptor descriptor )
    {
        try
        {
            oldProvider.openIndex( descriptor.identifier(), FulltextFactory.getType(descriptor) );
        }
        catch ( IOException e )
        {
            e.printStackTrace(  );
        }
        return oldProvider.getState( descriptor.identifier(), getType( descriptor )  );
    }

    @Override
    public IndexCapability getCapability( FulltextIndexDescriptor indexDescriptor )
    {
        return IndexCapability.NO_CAPABILITY;
    }

    @Override
    public boolean compatible( IndexDescriptor indexDescriptor )
    {
        return indexDescriptor instanceof FulltextIndexDescriptor;
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        //TODO store migration
        return StoreMigrationParticipant.NOT_PARTICIPATING;
    }
}
