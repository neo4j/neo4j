/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.convert;

import java.io.File;
import java.util.Collections;

import org.neo4j.coreedge.raft.replication.tx.LogIndexTxHeaderEncoding;
import org.neo4j.coreedge.raft.state.DurableStateStorageImporter;
import org.neo4j.coreedge.raft.state.id_allocation.IdAllocationState;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.KernelEventHandlers;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.kernel.impl.store.StoreFactory.*;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.ARRAY_BLOCK;
import static org.neo4j.kernel.impl.store.id.IdType.LABEL_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.LABEL_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.NEOSTORE_BLOCK;
import static org.neo4j.kernel.impl.store.id.IdType.NODE;
import static org.neo4j.kernel.impl.store.id.IdType.NODE_LABELS;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY_KEY_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY_KEY_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_GROUP;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_TYPE_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_TYPE_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.SCHEMA;
import static org.neo4j.kernel.impl.store.id.IdType.STRING_BLOCK;

public class ConvertClassicStoreCommand
{
    private File databaseDir;

    public ConvertClassicStoreCommand( File databaseDir )
    {
        this.databaseDir = databaseDir;
    }

    public void execute() throws Throwable
    {
        appendNullTransactionLogEntry( databaseDir );
        addIdAllocationState( databaseDir );
    }

    private void appendNullTransactionLogEntry( File dbDir) throws TransactionFailureException
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new EnterpriseGraphDatabaseFactory().newEmbeddedDatabase( dbDir );

        TransactionCommitProcess commitProcess = db.getDependencyResolver().resolveDependency( TransactionCommitProcess.class );

        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( Collections.emptyList() );
        byte[] txHeaderBytes = LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader( -1 );
        tx.setHeader( txHeaderBytes, -1, -1, -1, -1, -1, -1 );

        commitProcess.commit( new TransactionToApply( tx ), CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );

        db.shutdown();
    }

    private void addIdAllocationState( File dbDir ) throws Throwable
    {
        File clusterStateDirectory = new File( dbDir, "cluster-state" );

        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        DefaultIdGeneratorFactory factory = new DefaultIdGeneratorFactory( fileSystem );

        long[] highIds = new long[]{
                getHighId( dbDir, factory, NODE, NODE_STORE_NAME ),
                getHighId( dbDir, factory, RELATIONSHIP, RELATIONSHIP_STORE_NAME ),
                getHighId( dbDir, factory, PROPERTY, PROPERTY_STORE_NAME ),
                getHighId( dbDir, factory, STRING_BLOCK, PROPERTY_STRINGS_STORE_NAME ),
                getHighId( dbDir, factory, ARRAY_BLOCK, PROPERTY_ARRAYS_STORE_NAME ),
                getHighId( dbDir, factory, PROPERTY_KEY_TOKEN, PROPERTY_KEY_TOKEN_STORE_NAME ),
                getHighId( dbDir, factory, PROPERTY_KEY_TOKEN_NAME, PROPERTY_KEY_TOKEN_NAMES_STORE_NAME ),
                getHighId( dbDir, factory, RELATIONSHIP_TYPE_TOKEN, RELATIONSHIP_TYPE_TOKEN_STORE_NAME ),
                getHighId( dbDir, factory, RELATIONSHIP_TYPE_TOKEN_NAME, RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME ),
                getHighId( dbDir, factory, LABEL_TOKEN, LABEL_TOKEN_STORE_NAME ),
                getHighId( dbDir, factory, LABEL_TOKEN_NAME, LABEL_TOKEN_NAMES_STORE_NAME ),
                getHighId( dbDir, factory, NEOSTORE_BLOCK, "" ),
                getHighId( dbDir, factory, SCHEMA, SCHEMA_STORE_NAME ),
                getHighId( dbDir, factory, NODE_LABELS, NODE_LABELS_STORE_NAME ),
                getHighId( dbDir, factory, RELATIONSHIP_GROUP, RELATIONSHIP_GROUP_STORE_NAME )};

        IdAllocationState state = new IdAllocationState( highIds, -1 );

        DurableStateStorageImporter<IdAllocationState> storage = new DurableStateStorageImporter<>(
                fileSystem, new File( clusterStateDirectory, "id-allocation-state" ), "id-allocation",
                new IdAllocationState.Marshal(),
                1000, () -> new DatabaseHealth(
                new DatabasePanicEventGenerator( new KernelEventHandlers( NullLog.getInstance() ) ),
                NullLog.getInstance() ), NullLogProvider.getInstance() );

        storage.persist( state );

        storage.shutdown();
    }

    private long getHighId( File coreDir, DefaultIdGeneratorFactory factory, IdType idType, String store )
    {
        IdGenerator idGenerator = factory.open( new File( coreDir, idFile( store ) ), idType.getGrabSize(), idType, -1 );
        long highId = idGenerator.getHighId();
        idGenerator.close();
        return highId;
    }

    private String idFile( String store )
    {
        return MetaDataStore.DEFAULT_NAME + store + ".id";
    }
}
