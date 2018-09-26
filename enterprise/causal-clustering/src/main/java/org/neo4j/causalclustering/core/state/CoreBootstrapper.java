/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.recovery.RecoveryRequiredChecker;
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
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;
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

    private final DatabaseLayout databaseLayout;
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Config config;
    private final LogProvider logProvider;
    private final RecoveryRequiredChecker recoveryRequiredChecker;
    private final Log log;

    CoreBootstrapper( DatabaseLayout databaseLayout, PageCache pageCache, FileSystemAbstraction fs, Config config, LogProvider logProvider, Monitors monitors )
    {
        this.databaseLayout = databaseLayout;
        this.pageCache = pageCache;
        this.fs = fs;
        this.config = config;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.recoveryRequiredChecker = new RecoveryRequiredChecker( fs, pageCache, config, monitors );
    }

    public CoreSnapshot bootstrap( Set<MemberId> members ) throws Exception
    {
        if ( recoveryRequiredChecker.isRecoveryRequiredAt( databaseLayout ) )
        {
            String message = "Cannot bootstrap. Recovery is required. Please ensure that the store being seeded comes from a cleanly shutdown " +
                    "instance of Neo4j or a Neo4j backup";
            log.error( message );
            throw new IllegalStateException( message );
        }
        StoreFactory factory = new StoreFactory( databaseLayout, config,
                new DefaultIdGeneratorFactory( fs ), pageCache, fs, logProvider, EmptyVersionContextSupplier.EMPTY );

        NeoStores neoStores = factory.openAllNeoStores( true );
        neoStores.close();

        CoreSnapshot coreSnapshot = new CoreSnapshot( FIRST_INDEX, FIRST_TERM );
        coreSnapshot.add( CoreStateType.ID_ALLOCATION, deriveIdAllocationState( databaseLayout ) );
        coreSnapshot.add( CoreStateType.LOCK_TOKEN, new ReplicatedLockTokenState() );
        coreSnapshot.add( CoreStateType.RAFT_CORE_STATE,
                new RaftCoreState( new MembershipEntry( FIRST_INDEX, members ) ) );
        coreSnapshot.add( CoreStateType.SESSION_TRACKER, new GlobalSessionTrackerState() );
        appendNullTransactionLogEntryToSetRaftIndexToMinusOne();
        return coreSnapshot;
    }

    private void appendNullTransactionLogEntryToSetRaftIndexToMinusOne() throws IOException
    {
        ReadOnlyTransactionIdStore readOnlyTransactionIdStore = new ReadOnlyTransactionIdStore( pageCache, databaseLayout );
        LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( databaseLayout, fs, pageCache )
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

        File neoStoreFile = databaseLayout.metadataStore();
        MetaDataStore.setRecord( pageCache, neoStoreFile, LAST_TRANSACTION_ID, dummyTransactionId );
    }

    private IdAllocationState deriveIdAllocationState( DatabaseLayout databaseLayout )
    {
        DefaultIdGeneratorFactory factory = new DefaultIdGeneratorFactory( fs );

        long[] highIds = new long[]{
                getHighId( factory, NODE, databaseLayout.idNodeStore() ),
                getHighId( factory, RELATIONSHIP, databaseLayout.idRelationshipStore() ),
                getHighId( factory, PROPERTY, databaseLayout.idPropertyStore() ),
                getHighId( factory, STRING_BLOCK, databaseLayout.idPropertyStringStore() ),
                getHighId( factory, ARRAY_BLOCK, databaseLayout.idPropertyArrayStore() ),
                getHighId( factory, PROPERTY_KEY_TOKEN, databaseLayout.idPropertyKeyTokenStore() ),
                getHighId( factory, PROPERTY_KEY_TOKEN_NAME, databaseLayout.idPropertyKeyTokenNamesStore() ),
                getHighId( factory, RELATIONSHIP_TYPE_TOKEN, databaseLayout.idRelationshipTypeTokenStore() ),
                getHighId( factory, RELATIONSHIP_TYPE_TOKEN_NAME, databaseLayout.idRelationshipTypeTokenNamesStore() ),
                getHighId( factory, LABEL_TOKEN, databaseLayout.idLabelTokenStore() ),
                getHighId( factory, LABEL_TOKEN_NAME, databaseLayout.idLabelTokenNamesStore() ),
                getHighId( factory, NEOSTORE_BLOCK, databaseLayout.idMetadataStore() ),
                getHighId( factory, SCHEMA, databaseLayout.idSchemaStore() ),
                getHighId( factory, NODE_LABELS, databaseLayout.idNodeLabelStore() ),
                getHighId( factory, RELATIONSHIP_GROUP, databaseLayout.idRelationshipGroupStore() )};

        return new IdAllocationState( highIds, FIRST_INDEX );
    }

    private static long getHighId( DefaultIdGeneratorFactory factory, IdType idType, File idFile )
    {
        IdGenerator idGenerator = factory.open( idFile, idType, () -> -1L, Long.MAX_VALUE );
        long highId = idGenerator.getHighId();
        idGenerator.close();
        return highId;
    }
}
