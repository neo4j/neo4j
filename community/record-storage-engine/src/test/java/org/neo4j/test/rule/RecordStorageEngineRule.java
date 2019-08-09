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
package org.neo4j.test.rule;

import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.id.DefaultIdController;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.BatchTransactionApplierFacade;
import org.neo4j.internal.recordstorage.IndexActivator;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.internal.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.lock.LockService;
import org.neo4j.lock.ReentrantLockService;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseEventListeners;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.DatabasePanicEventGenerator;
import org.neo4j.monitoring.Health;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.NodeLabelUpdateListener;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;

/**
 * Conveniently manages a {@link RecordStorageEngine} in a test. Needs {@link FileSystemAbstraction} and
 * {@link PageCache}, which usually are managed by test rules themselves. That's why they are passed in
 * when {@link #getWith(FileSystemAbstraction, PageCache, DatabaseLayout) getting (constructing)} the engine. Further
 * dependencies can be overridden in that returned builder as well.
 * <p>
 * Keep in mind that this rule must be created BEFORE page cache rule and any file system rule so that shutdown order gets correct.
 */
public class RecordStorageEngineRule extends ExternalResource
{
    private final LifeSupport life = new LifeSupport();

    @Override
    public void before() throws Throwable
    {
        super.before();
        life.start();
    }

    public Builder getWith( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout databaseLayout )
    {
        return new Builder( fs, pageCache, databaseLayout );
    }

    private RecordStorageEngine get( FileSystemAbstraction fs, PageCache pageCache, Health databaseHealth,
            DatabaseLayout databaseLayout, Function<BatchTransactionApplierFacade,BatchTransactionApplierFacade> transactionApplierTransformer,
            IndexUpdateListener indexUpdateListener, NodeLabelUpdateListener nodeLabelUpdateListener, LockService lockService, TokenHolders tokenHolders,
            Config config, ConstraintRuleAccessor constraintSemantics, IndexConfigCompleter indexConfigCompleter )
    {
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs, immediate() );
        NullLogProvider nullLogProvider = NullLogProvider.getInstance();
        RecordStorageEngine engine = life.add(
                new ExtendedRecordStorageEngine( databaseLayout, config, pageCache, fs, nullLogProvider, tokenHolders, mock( SchemaState.class ),
                        constraintSemantics, indexConfigCompleter, lockService, databaseHealth, idGeneratorFactory,
                        new DefaultIdController(), transactionApplierTransformer ) );
        engine.addIndexUpdateListener( indexUpdateListener );
        engine.addNodeLabelUpdateListener( nodeLabelUpdateListener );
        return engine;
    }

    @Override
    public void after( boolean successful ) throws Throwable
    {
        life.shutdown();
        super.after( successful );
    }

    public class Builder
    {
        private final FileSystemAbstraction fs;
        private final PageCache pageCache;
        private Health databaseHealth = new DatabaseHealth(
                new DatabasePanicEventGenerator( new DatabaseEventListeners( NullLog.getInstance() ), DEFAULT_DATABASE_NAME ),
                NullLog.getInstance() );
        private final DatabaseLayout databaseLayout;
        private Function<BatchTransactionApplierFacade,BatchTransactionApplierFacade> transactionApplierTransformer =
                applierFacade -> applierFacade;
        private IndexUpdateListener indexUpdateListener = new IndexUpdateListener.Adapter();
        private NodeLabelUpdateListener nodeLabelUpdateListener = new NodeLabelUpdateListener.Adapter();
        private LockService lockService = new ReentrantLockService();
        private TokenHolders tokenHolders = new TokenHolders( mock( TokenHolder.class ), mock( TokenHolder.class ), mock( TokenHolder.class ) );
        private Config config = Config.defaults();
        private ConstraintRuleAccessor constraintSemantics = new ConstraintRuleAccessor()
        {
            @Override
            public ConstraintDescriptor readConstraint( ConstraintDescriptor rule )
            {
                return rule;
            }

            @Override
            public ConstraintDescriptor createUniquenessConstraintRule( long ruleId, UniquenessConstraintDescriptor descriptor, long indexId )
            {
                return descriptor.withId( ruleId ).withOwnedIndexId( indexId );
            }

            @Override
            public ConstraintDescriptor createNodeKeyConstraintRule( long ruleId, NodeKeyConstraintDescriptor descriptor, long indexId )
            {
                throw new UnsupportedOperationException( "Not needed a.t.m." );
            }

            @Override
            public ConstraintDescriptor createExistenceConstraint( long ruleId, ConstraintDescriptor descriptor )
            {
                throw new UnsupportedOperationException( "Not needed a.t.m." );
            }
        };
        private IndexConfigCompleter indexConfigCompleter = index -> index;

        public Builder( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout databaseLayout )
        {
            this.fs = fs;
            this.pageCache = pageCache;
            this.databaseLayout = databaseLayout;
        }

        public Builder transactionApplierTransformer(
                Function<BatchTransactionApplierFacade,BatchTransactionApplierFacade> transactionApplierTransformer )
        {
            this.transactionApplierTransformer = transactionApplierTransformer;
            return this;
        }

        public Builder databaseHealth( Health databaseHealth )
        {
            this.databaseHealth = databaseHealth;
            return this;
        }

        public Builder indexUpdateListener( IndexUpdateListener indexUpdateListener )
        {
            this.indexUpdateListener = indexUpdateListener;
            return this;
        }

        public Builder nodeLabelUpdateListener( NodeLabelUpdateListener nodeLabelUpdateListener )
        {
            this.nodeLabelUpdateListener = nodeLabelUpdateListener;
            return this;
        }

        public Builder lockService( LockService lockService )
        {
            this.lockService = lockService;
            return this;
        }

        public Builder tokenHolders( TokenHolders tokenHolders )
        {
            this.tokenHolders = tokenHolders;
            return this;
        }

        public <T> Builder setting( Setting<T> setting, T value )
        {
            config.set( setting, value );
            return this;
        }

        public Builder constraintSemantics( ConstraintRuleAccessor constraintSemantics )
        {
            this.constraintSemantics = constraintSemantics;
            return this;
        }

        public Builder indexConfigCompleter( IndexConfigCompleter indexConfigCompleter )
        {
            this.indexConfigCompleter = indexConfigCompleter;
            return this;
        }

        public RecordStorageEngine build()
        {
            return get( fs, pageCache, databaseHealth, databaseLayout, transactionApplierTransformer, indexUpdateListener,
                    nodeLabelUpdateListener, lockService, tokenHolders, config, constraintSemantics, indexConfigCompleter );
        }
    }

    private static class ExtendedRecordStorageEngine extends RecordStorageEngine
    {
        private final Function<BatchTransactionApplierFacade,BatchTransactionApplierFacade>
                transactionApplierTransformer;

        ExtendedRecordStorageEngine( DatabaseLayout databaseLayout, Config config, PageCache pageCache, FileSystemAbstraction fs,
                LogProvider logProvider, TokenHolders tokenHolders, SchemaState schemaState,
                ConstraintRuleAccessor constraintSemantics,
                IndexConfigCompleter indexConfigCompleter,
                LockService lockService, Health databaseHealth,
                IdGeneratorFactory idGeneratorFactory, IdController idController,
                Function<BatchTransactionApplierFacade,BatchTransactionApplierFacade> transactionApplierTransformer )
        {
            super( databaseLayout, config, pageCache, fs, logProvider, tokenHolders, schemaState, constraintSemantics, indexConfigCompleter,
                    lockService, databaseHealth, idGeneratorFactory, idController, EmptyVersionContextSupplier.EMPTY, true );
            this.transactionApplierTransformer = transactionApplierTransformer;
        }

        @Override
        protected BatchTransactionApplierFacade applier( TransactionApplicationMode mode, IndexActivator indexActivator )
        {
            BatchTransactionApplierFacade recordEngineApplier = super.applier( mode, indexActivator );
            return transactionApplierTransformer.apply( recordEngineApplier );
        }
    }
}
