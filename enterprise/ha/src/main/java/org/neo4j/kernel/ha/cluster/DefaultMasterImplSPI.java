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

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.neo4j.com.RequestContext;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.com.AccumulatorVisitor;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.xaframework.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

class DefaultMasterImplSPI implements MasterImpl.SPI
{
    private static final int ID_GRAB_SIZE = 1000;
    private final DependencyResolver dependencyResolver;
    private final GraphDatabaseAPI graphDb;
    private final Logging logging;
    private final Monitors monitors;
    private LogicalTransactionStore txStore;

    public DefaultMasterImplSPI( GraphDatabaseAPI graphDb, Logging logging, Monitors monitors )
    {
        this.graphDb = graphDb;
        this.logging = logging;
        this.dependencyResolver = graphDb.getDependencyResolver();
        this.monitors = monitors;
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
    public void applyPreparedTransaction( TransactionRepresentation preparedTransaction ) throws IOException
    {
        try
        {
            txStore.getAppender().append( preparedTransaction ).get();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
        catch ( ExecutionException e )
        {
            throw new RuntimeException( e );
        }
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
    public RequestContext rotateLogsAndStreamStoreFiles( StoreWriter writer )
    {
        // TODO 2.2-future
        return null;
    }

    @Override
    public Response<Void> copyTransactions( String dsName, long startTxId, long endTxId )
    {
        // 2.2-future unnecessary
        return null;
    }

    @Override
    public <T> Response<T> packResponse( RequestContext context, T response, Predicate<Long> filter )
    {
        try
        {
            AccumulatorVisitor<CommittedTransactionRepresentation> accumulator = new AccumulatorVisitor<>();
            txStore.getCursor( context.lastAppliedTransaction() + 1, accumulator );
            Iterable<CommittedTransactionRepresentation> txs = accumulator.getAccumulator();

            return new Response<>( response,
                    storeId(),
                    txs,
                    ResourceReleaser.NO_OP );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void pushTransaction( int eventIdentifier, long tx, int machineId )
    {
        // 2.2-future - unnecessary
    }

    private <T> T resolve( Class<T> dependencyType )
    {
        return dependencyResolver.resolveDependency( dependencyType );
    }

}
