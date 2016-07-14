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
package org.neo4j.coreedge;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.raft.LeaderLocator;
import org.neo4j.coreedge.raft.replication.RaftReplicator;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationStateMachine;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdGeneratorFactory;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdRangeAcquirer;
import org.neo4j.coreedge.raft.replication.token.ReplicatedLabelTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedPropertyKeyTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedRelationshipTypeTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenStateMachine;
import org.neo4j.coreedge.raft.replication.token.TokenRegistry;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionCommitProcess;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionStateMachine;
import org.neo4j.coreedge.raft.state.CoreStateMachines;
import org.neo4j.coreedge.raft.state.DurableStateStorage;
import org.neo4j.coreedge.raft.state.StateStorage;
import org.neo4j.coreedge.raft.state.id_allocation.IdAllocationState;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.core.RecoverTransactionLogState;
import org.neo4j.coreedge.server.core.locks.LeaderOnlyLockManager;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenState;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenStateMachine;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.enterprise.id.EnterpriseIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.impl.store.stats.IdBasedStoreEntityCounters;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.Token;

public class CoreStateMachinesModule
{
    public final IdGeneratorFactory idGeneratorFactory;
    public final IdTypeConfigurationProvider idTypeConfigurationProvider;
    public final LabelTokenHolder labelTokenHolder;
    public final PropertyKeyTokenHolder propertyKeyTokenHolder;
    public final RelationshipTypeTokenHolder relationshipTypeTokenHolder;
    public final Locks lockManager;
    public final CommitProcessFactory commitProcessFactory;

    public final ReplicatedIdGeneratorFactory replicatedIdGeneratorFactory;
    public final CoreStateMachines coreStateMachines;

    public CoreStateMachinesModule( CoreMember myself, PlatformModule platformModule, File clusterStateDirectory,
                                    Supplier<DatabaseHealth> databaseHealthSupplier, Config config,
                                    RaftReplicator replicator, LeaderLocator leaderLocator,
                                    Dependencies dependencies, LocalDatabase localDatabase )
    {
        StateStorage<IdAllocationState> idAllocationState;
        StateStorage<ReplicatedLockTokenState> lockTokenState;
        final LifeSupport life = platformModule.life;
        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        LogService logging = platformModule.logging;
        LogProvider logProvider = logging.getInternalLogProvider();

        try
        {
            lockTokenState = life.add(
                    new DurableStateStorage<>( fileSystem, new File( clusterStateDirectory, "lock-token-state" ),
                            "lock-token", new ReplicatedLockTokenState.Marshal( new CoreMember.CoreMemberMarshal() ),
                            config.get( CoreEdgeClusterSettings.replicated_lock_token_state_size ),
                            databaseHealthSupplier, logProvider ) );

            idAllocationState = life.add(
                    new DurableStateStorage<>( fileSystem, new File( clusterStateDirectory, "id-allocation-state" ),
                            "id-allocation", new IdAllocationState.Marshal(),
                            config.get( CoreEdgeClusterSettings.id_alloc_state_size ), databaseHealthSupplier,
                            logProvider ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        ReplicatedIdAllocationStateMachine idAllocationStateMachine =
                new ReplicatedIdAllocationStateMachine( idAllocationState );

        int allocationChunk = 1024; // TODO: AllocationChunk should be configurable and per type.
        ReplicatedIdRangeAcquirer idRangeAcquirer =
                new ReplicatedIdRangeAcquirer( replicator, idAllocationStateMachine, allocationChunk, myself,
                        logProvider );

        idTypeConfigurationProvider = new EnterpriseIdTypeConfigurationProvider( config );
        replicatedIdGeneratorFactory = createIdGeneratorFactory( fileSystem, idRangeAcquirer, logProvider,
                idTypeConfigurationProvider );

        this.idGeneratorFactory = dependencies.satisfyDependency( replicatedIdGeneratorFactory );
        dependencies.satisfyDependency( new IdBasedStoreEntityCounters( this.idGeneratorFactory ) );

        Long tokenCreationTimeout = config.get( CoreEdgeClusterSettings.token_creation_timeout );

        TokenRegistry<RelationshipTypeToken> relationshipTypeTokenRegistry = new TokenRegistry<>( "RelationshipType" );
        ReplicatedRelationshipTypeTokenHolder relationshipTypeTokenHolder =
                new ReplicatedRelationshipTypeTokenHolder( relationshipTypeTokenRegistry, replicator,
                        this.idGeneratorFactory, dependencies, tokenCreationTimeout );

        TokenRegistry<Token> propertyKeyTokenRegistry = new TokenRegistry<>( "PropertyKey" );
        ReplicatedPropertyKeyTokenHolder propertyKeyTokenHolder =
                new ReplicatedPropertyKeyTokenHolder( propertyKeyTokenRegistry, replicator, this.idGeneratorFactory,
                        dependencies, tokenCreationTimeout );

        TokenRegistry<Token> labelTokenRegistry = new TokenRegistry<>( "Label" );
        ReplicatedLabelTokenHolder labelTokenHolder =
                new ReplicatedLabelTokenHolder( labelTokenRegistry, replicator, this.idGeneratorFactory, dependencies,
                        tokenCreationTimeout );

        ReplicatedLockTokenStateMachine replicatedLockTokenStateMachine =
                new ReplicatedLockTokenStateMachine( lockTokenState );

        RecoverTransactionLogState txLogState = new RecoverTransactionLogState( dependencies, logProvider );

        ReplicatedTokenStateMachine<Token> labelTokenStateMachine =
                new ReplicatedTokenStateMachine<>( labelTokenRegistry, new Token.Factory(), logProvider );

        ReplicatedTokenStateMachine<Token> propertyKeyTokenStateMachine =
                new ReplicatedTokenStateMachine<>( propertyKeyTokenRegistry, new Token.Factory(), logProvider );

        ReplicatedTokenStateMachine<RelationshipTypeToken> relationshipTypeTokenStateMachine =
                new ReplicatedTokenStateMachine<>( relationshipTypeTokenRegistry, new RelationshipTypeToken.Factory(),
                        logProvider );

        ReplicatedTransactionStateMachine replicatedTxStateMachine =
                new ReplicatedTransactionStateMachine( replicatedLockTokenStateMachine,
                        config.get( CoreEdgeClusterSettings.state_machine_apply_max_batch_size ), logProvider );

        dependencies.satisfyDependencies( replicatedTxStateMachine );

        long leaderLockTokenTimeout = config.get( CoreEdgeClusterSettings.leader_lock_token_timeout );
        lockManager = createLockManager( config, logging, replicator, myself, leaderLocator, leaderLockTokenTimeout,
                replicatedLockTokenStateMachine );

        coreStateMachines = new CoreStateMachines( replicatedTxStateMachine, labelTokenStateMachine,
                relationshipTypeTokenStateMachine, propertyKeyTokenStateMachine, replicatedLockTokenStateMachine,
                idAllocationStateMachine, txLogState, localDatabase );

        commitProcessFactory = ( appender, applier, ignored ) -> {
            TransactionRepresentationCommitProcess localCommit =
                    new TransactionRepresentationCommitProcess( appender, applier );
            coreStateMachines.refresh( localCommit ); // This gets called when a core-to-core download is performed.
            return new ReplicatedTransactionCommitProcess( replicator );
        };

        this.relationshipTypeTokenHolder = relationshipTypeTokenHolder;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
    }

    private ReplicatedIdGeneratorFactory createIdGeneratorFactory(
            FileSystemAbstraction fileSystem,
            final ReplicatedIdRangeAcquirer idRangeAcquirer,
            final LogProvider logProvider,
            IdTypeConfigurationProvider idTypeConfigurationProvider )
    {
        return new ReplicatedIdGeneratorFactory( fileSystem, idRangeAcquirer, logProvider,
                idTypeConfigurationProvider );
    }

    private Locks createLockManager( final Config config, final LogService logging, final Replicator replicator,
                                     CoreMember myself, LeaderLocator leaderLocator, long leaderLockTokenTimeout,
                                     ReplicatedLockTokenStateMachine lockTokenStateMachine )
    {
        Locks localLocks = CommunityEditionModule.createLockManager( config, logging );

        return new LeaderOnlyLockManager( myself, replicator, leaderLocator, localLocks, leaderLockTokenTimeout,
                lockTokenStateMachine );
    }
}
