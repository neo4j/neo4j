/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.recordstorage;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.function.ThrowingBiConsumer;
import org.neo4j.internal.id.DefaultIdController;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.Health;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.StoreTokens.createReadOnlyTokenHolder;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;

public class RecordStorageEngineTestUtils
{
    public static RecordStorageEngine openSimpleStorageEngine( FileSystemAbstraction fs, PageCache pageCache, RecordDatabaseLayout layout, Config config )
    {
        TokenHolders tokenHolders = new TokenHolders(
                createReadOnlyTokenHolder( TokenHolder.TYPE_PROPERTY_KEY ),
                createReadOnlyTokenHolder( TokenHolder.TYPE_LABEL ),
                createReadOnlyTokenHolder( TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        return new RecordStorageEngine( layout, config, pageCache, fs, NullLogProvider.getInstance(), NullLogProvider.getInstance(), tokenHolders,
                mock( SchemaState.class ), new StandardConstraintRuleAccessor(), c -> c, NO_LOCK_SERVICE, mock( Health.class ),
                new DefaultIdGeneratorFactory( fs, immediate(), DEFAULT_DATABASE_NAME ), new DefaultIdController(), immediate(), PageCacheTracer.NULL, true,
                EmptyMemoryTracker.INSTANCE, writable(), CommandLockVerification.Factory.IGNORE, LockVerificationMonitor.Factory.IGNORE, NULL_CONTEXT );
    }

    public static void applyLogicalChanges( RecordStorageEngine storageEngine, ThrowingBiConsumer<ReadableTransactionState,TxStateVisitor,Exception> changes )
            throws Exception
    {
        ReadableTransactionState txState = mock( ReadableTransactionState.class );
        doAnswer( invocationOnMock ->
        {
            TxStateVisitor visitor = invocationOnMock.getArgument( 0 );
            changes.accept( txState, visitor );
            return null;
        } ).when( txState ).accept( any() );
        List<StorageCommand> commands = new ArrayList<>();
        NeoStores neoStores = storageEngine.testAccessNeoStores();
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();
        CursorContext cursorContext = NULL_CONTEXT;
        try ( RecordStorageCommandCreationContext commandCreationContext = storageEngine.newCommandCreationContext( EmptyMemoryTracker.INSTANCE );
              StoreCursors storeCursors = new CachedStoreCursors( neoStores, cursorContext ) )
        {
            commandCreationContext.initialize( cursorContext, storeCursors );
            storageEngine.createCommands( commands, txState, storageEngine.newReader(), commandCreationContext, ResourceLocker.IGNORE, LockTracer.NONE,
                    metaDataStore.getLastCommittedTransactionId(), t -> t, cursorContext, storeCursors, EmptyMemoryTracker.INSTANCE );
            storageEngine.apply( new GroupOfCommands( metaDataStore.nextCommittingTransactionId(), storeCursors,
                            commands.toArray( new StorageCommand[0] ) ), TransactionApplicationMode.EXTERNAL );
        }
    }
}
