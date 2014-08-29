/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster;

import java.io.File;
import java.io.IOException;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.ResponsePacker;
import org.neo4j.com.storecopy.StoreCopyServer;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.nioneo.xa.DataSourceManager;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;

class DefaultMasterImplSPI implements MasterImpl.SPI
{
    private static final int ID_GRAB_SIZE = 1000;
    private final DependencyResolver dependencyResolver;
    private final GraphDatabaseAPI graphDb;
    private final LogicalTransactionStore txStore;
    private final TransactionIdStore transactionIdStore;
    private final FileSystemAbstraction fileSystem;
    private final File storeDir;
    private final ResponsePacker responsePacker;

    public DefaultMasterImplSPI( GraphDatabaseAPI graphDb )
    {
        this.graphDb = graphDb;
        this.dependencyResolver = graphDb.getDependencyResolver();

        // Hmm, fetching the dependencies here instead of handing them in the constructor directly feels bad,
        // but it seems like there's some intricate usage and need for the db's dependency resolver.
        this.transactionIdStore = dependencyResolver.resolveDependency( TransactionIdStore.class );
        this.fileSystem = dependencyResolver.resolveDependency( FileSystemAbstraction.class );
        this.storeDir = new File( graphDb.getStoreDir() );
        this.txStore = dependencyResolver.resolveDependency( LogicalTransactionStore.class );
        this.responsePacker = new ResponsePacker( txStore, transactionIdStore, graphDb );
    }

    @Override
    public boolean isAccessible()
    {
        // Wait for 5s for the database to become available, if not already so
        return graphDb.isAvailable( 5000 );
    }

    @Override
    public int getOrCreateLabel( String name )
    {
        LabelTokenHolder labels = resolve( LabelTokenHolder.class );
        return labels.getOrCreateId( name );
    }

    @Override
    public int getOrCreateProperty( String name )
    {
        PropertyKeyTokenHolder propertyKeyHolder = resolve( PropertyKeyTokenHolder.class );
        return propertyKeyHolder.getOrCreateId( name );
    }

    @Override
    public Locks.Client acquireClient()
    {
        return resolve( Locks.class ).newClient();
    }

    @Override
    public IdAllocation allocateIds( IdType idType )
    {
        IdGenerator generator = resolve( IdGeneratorFactory.class ).get(idType);
        return new IdAllocation( generator.nextIdBatch( ID_GRAB_SIZE ), generator.getHighId(),
                generator.getDefragCount() );
    }

    @Override
    public StoreId storeId()
    {
        return graphDb.storeId();
    }

    @Override
    public long applyPreparedTransaction( TransactionRepresentation preparedTransaction ) throws IOException,
            TransactionFailureException
    {

        TransactionCommitProcess txCommitProcess = dependencyResolver.resolveDependency( NeoStoreXaDataSource.class ).
                getDependencyResolver().resolveDependency( TransactionCommitProcess.class );
        return txCommitProcess.commit( preparedTransaction );
    }

    @Override
    public Integer createRelationshipType( String name )
    {
        return resolve(RelationshipTypeTokenHolder.class).getOrCreateId( name );
    }

    @Override
    public Pair<Integer, Long> getMasterIdForCommittedTx( long txId ) throws IOException
    {
        TransactionMetadataCache.TransactionMetadata metadata = txStore.getMetadataFor( txId );
        return Pair.of( metadata.getMasterId(), metadata.getChecksum() );
    }

    @Override
    public RequestContext flushStoresAndStreamStoreFiles( StoreWriter writer )
    {
        NeoStoreXaDataSource dataSource = graphDb.getDependencyResolver().resolveDependency(
                DataSourceManager.class ).getDataSource();
        StoreCopyServer streamer = new StoreCopyServer( transactionIdStore, dataSource, fileSystem, storeDir );
        return streamer.flushStoresAndStreamStoreFiles( writer );
    }

    @Override
    public <T> Response<T> packResponse( RequestContext context, T response, Predicate<Long> filter )
    {
        return responsePacker.packResponse( context, response, wrapLongFilter( filter ) );
    }

    // TODO there should be no need to wrap this here, provide the proper predicate type from the outside
    // directly instead
    private Predicate<CommittedTransactionRepresentation> wrapLongFilter( final Predicate<Long> filter )
    {
        return new Predicate<CommittedTransactionRepresentation>()
        {
            @Override
            public boolean accept( CommittedTransactionRepresentation transaction )
            {
                return filter.accept( transaction.getCommitEntry().getTxId() );
            }
        };
    }

    private <T> T resolve( Class<T> dependencyType )
    {
        return dependencyResolver.resolveDependency( dependencyType );
    }
}
