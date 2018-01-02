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
package org.neo4j.causalclustering.core.state;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import org.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import org.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import org.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateType;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;
import static org.neo4j.kernel.impl.store.StoreFactory.LABEL_TOKEN_NAMES_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.LABEL_TOKEN_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.NODE_LABELS_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.NODE_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_ARRAYS_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_STRINGS_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_GROUP_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.SCHEMA_STORE_NAME;
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

public class CoreBootstrapper
{
    private static final long FIRST_INDEX = 0L;
    private static final long FIRST_TERM = 0L;

    private final File storeDir;
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Config config;
    private final LogProvider logProvider;

    CoreBootstrapper( File storeDir, PageCache pageCache, FileSystemAbstraction fs, Config config, LogProvider logProvider )
    {
        this.storeDir = storeDir;
        this.pageCache = pageCache;
        this.fs = fs;
        this.config = config;
        this.logProvider = logProvider;
    }

    public CoreSnapshot bootstrap( Set<MemberId> members ) throws IOException
    {
        StoreFactory factory = new StoreFactory( storeDir, config,
                new DefaultIdGeneratorFactory( fs ), pageCache, fs, logProvider );

        NeoStores neoStores = factory.openAllNeoStores( true );
        neoStores.close();

        CoreSnapshot coreSnapshot = new CoreSnapshot( FIRST_INDEX, FIRST_TERM );
        coreSnapshot.add( CoreStateType.ID_ALLOCATION, deriveIdAllocationState( storeDir ) );
        coreSnapshot.add( CoreStateType.LOCK_TOKEN, new ReplicatedLockTokenState() );
        coreSnapshot.add( CoreStateType.RAFT_CORE_STATE,
                new RaftCoreState( new MembershipEntry( FIRST_INDEX, members ) ) );
        coreSnapshot.add( CoreStateType.SESSION_TRACKER, new GlobalSessionTrackerState() );
        appendNullTransactionLogEntryToSetRaftIndexToMinusOne();
        return coreSnapshot;
    }

    private void appendNullTransactionLogEntryToSetRaftIndexToMinusOne() throws IOException
    {
        ReadOnlyTransactionIdStore readOnlyTransactionIdStore = new ReadOnlyTransactionIdStore( pageCache, storeDir );
        LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( storeDir, fs, pageCache )
                .withConfig( config )
                .withLastCommittedTransactionIdSupplier( () -> readOnlyTransactionIdStore.getLastClosedTransactionId() - 1 )
                .build();

        long dummyTransactionId;
        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            FlushableChannel channel = logFiles.getLogFile().getWriter();
            TransactionLogWriter writer = new TransactionLogWriter( new LogEntryWriter( channel ) );

            long lastCommittedTransactionId = readOnlyTransactionIdStore.getLastCommittedTransactionId();
            PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( Collections.emptyList() );
            byte[] txHeaderBytes = LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader( -1 );
            tx.setHeader( txHeaderBytes, -1, -1, -1, lastCommittedTransactionId, -1, -1 );

            dummyTransactionId = lastCommittedTransactionId + 1;
            writer.append( tx, dummyTransactionId );
            channel.prepareForFlush().flush();
        }

        File neoStoreFile = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        MetaDataStore.setRecord( pageCache, neoStoreFile, LAST_TRANSACTION_ID, dummyTransactionId );
    }

    private IdAllocationState deriveIdAllocationState( File dbDir ) throws IOException
    {
        DefaultIdGeneratorFactory factory = new DefaultIdGeneratorFactory( fs );

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

        return new IdAllocationState( highIds, FIRST_INDEX );
    }

    private long getHighId( File coreDir, DefaultIdGeneratorFactory factory, IdType idType, String store )
    {
        IdGenerator idGenerator = factory.open( new File( coreDir, idFile( store ) ), idType, () -> -1L, Long.MAX_VALUE );
        long highId = idGenerator.getHighId();
        idGenerator.close();
        return highId;
    }

    private static String idFile( String store )
    {
        return MetaDataStore.DEFAULT_NAME + store + ".id";
    }
}
