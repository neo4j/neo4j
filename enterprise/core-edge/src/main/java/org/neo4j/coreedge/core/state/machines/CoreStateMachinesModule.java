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
package org.neo4j.coreedge.core.state.machines;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.core.consensus.LeaderLocator;
import org.neo4j.coreedge.core.replication.RaftReplicator;
import org.neo4j.coreedge.core.replication.Replicator;
import org.neo4j.coreedge.core.state.machines.id.IdAllocationState;
import org.neo4j.coreedge.core.state.machines.id.ReplicatedIdAllocationStateMachine;
import org.neo4j.coreedge.core.state.machines.id.ReplicatedIdGeneratorFactory;
import org.neo4j.coreedge.core.state.machines.id.ReplicatedIdRangeAcquirer;
import org.neo4j.coreedge.core.state.machines.locks.LeaderOnlyLockManager;
import org.neo4j.coreedge.core.state.machines.locks.ReplicatedLockTokenState;
import org.neo4j.coreedge.core.state.machines.locks.ReplicatedLockTokenStateMachine;
import org.neo4j.coreedge.core.state.machines.token.ReplicatedLabelTokenHolder;
import org.neo4j.coreedge.core.state.machines.token.ReplicatedPropertyKeyTokenHolder;
import org.neo4j.coreedge.core.state.machines.token.ReplicatedRelationshipTypeTokenHolder;
import org.neo4j.coreedge.core.state.machines.token.ReplicatedTokenStateMachine;
import org.neo4j.coreedge.core.state.machines.token.TokenRegistry;
import org.neo4j.coreedge.core.state.machines.tx.RecoverTransactionLogState;
import org.neo4j.coreedge.core.state.machines.tx.ReplicatedTransactionCommitProcess;
import org.neo4j.coreedge.core.state.machines.tx.ReplicatedTransactionStateMachine;
import org.neo4j.coreedge.core.state.storage.DurableStateStorage;
import org.neo4j.coreedge.core.state.storage.StateStorage;
import org.neo4j.coreedge.identity.MemberId;
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
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.impl.store.stats.IdBasedStoreEntityCounters;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.Token;

import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.array_block_id_allocation_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.id_alloc_state_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.label_token_id_allocation_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.label_token_name_id_allocation_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.leader_lock_token_timeout;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.neostore_block_id_allocation_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.node_id_allocation_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.node_labels_id_allocation_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.property_id_allocation_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.property_key_token_id_allocation_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.property_key_token_name_id_allocation_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.relationship_group_id_allocation_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.relationship_id_allocation_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.relationship_type_token_id_allocation_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.relationship_type_token_name_id_allocation_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.replicated_lock_token_state_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.schema_id_allocation_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.state_machine_apply_max_batch_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.string_block_id_allocation_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.token_creation_timeout;

public class CoreStateMachinesModule
{
    public static final String ID_ALLOCATION_NAME = "id-allocation";
    public static final String LOCK_TOKEN_NAME = "lock-token";

    public final ReplicatedIdGeneratorFactory idGeneratorFactory;
    public final IdTypeConfigurationProvider idTypeConfigurationProvider;
    public final LabelTokenHolder labelTokenHolder;
    public final PropertyKeyTokenHolder propertyKeyTokenHolder;
    public final RelationshipTypeTokenHolder relationshipTypeTokenHolder;
    public final Locks lockManager;
    public final CommitProcessFactory commitProcessFactory;

    public final CoreStateMachines coreStateMachines;

    public CoreStateMachinesModule( MemberId myself, PlatformModule platformModule, File clusterStateDirectory,
            Config config, RaftReplicator replicator, LeaderLocator leaderLocator, Dependencies dependencies,
            LocalDatabase localDatabase )
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
                    new DurableStateStorage<>( fileSystem, clusterStateDirectory, LOCK_TOKEN_NAME,
                            new ReplicatedLockTokenState.Marshal( new MemberId.Marshal() ),
                            config.get( replicated_lock_token_state_size ), logProvider ) );

            idAllocationState = life.add(
                    new DurableStateStorage<>( fileSystem, clusterStateDirectory, ID_ALLOCATION_NAME,
                            new IdAllocationState.Marshal(),
                            config.get( id_alloc_state_size ), logProvider ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        ReplicatedIdAllocationStateMachine idAllocationStateMachine =
                new ReplicatedIdAllocationStateMachine( idAllocationState );

        Map<IdType,Integer> allocationSizes = getIdTypeAllocationSizeFromConfig( config );

        ReplicatedIdRangeAcquirer idRangeAcquirer =
                new ReplicatedIdRangeAcquirer( replicator, idAllocationStateMachine, allocationSizes, myself,
                        logProvider );

        idTypeConfigurationProvider = new EnterpriseIdTypeConfigurationProvider( config );

        this.idGeneratorFactory = dependencies.satisfyDependency( createIdGeneratorFactory( fileSystem,
                idRangeAcquirer, logProvider,
                idTypeConfigurationProvider ) );

        life.add( this.idGeneratorFactory );

        dependencies.satisfyDependency( new IdBasedStoreEntityCounters( this.idGeneratorFactory ) );

        Long tokenCreationTimeout = config.get( token_creation_timeout );

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
                        config.get( state_machine_apply_max_batch_size ), logProvider );

        dependencies.satisfyDependencies( replicatedTxStateMachine );

        long leaderLockTokenTimeout = config.get( leader_lock_token_timeout );
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

    private Map<IdType,Integer> getIdTypeAllocationSizeFromConfig( Config config )
    {
        Map<IdType,Integer> allocationSizes = new HashMap<>( IdType.values().length );
        allocationSizes.put( IdType.NODE, config.get( node_id_allocation_size ) );
        allocationSizes.put( IdType.RELATIONSHIP, config.get( relationship_id_allocation_size ) );
        allocationSizes.put( IdType.PROPERTY, config.get( property_id_allocation_size ) );
        allocationSizes.put( IdType.STRING_BLOCK, config.get( string_block_id_allocation_size ) );
        allocationSizes.put( IdType.ARRAY_BLOCK, config.get( array_block_id_allocation_size ) );
        allocationSizes.put( IdType.PROPERTY_KEY_TOKEN, config.get( property_key_token_id_allocation_size ) );
        allocationSizes.put( IdType.PROPERTY_KEY_TOKEN_NAME, config.get( property_key_token_name_id_allocation_size ) );
        allocationSizes.put( IdType.RELATIONSHIP_TYPE_TOKEN, config.get( relationship_type_token_id_allocation_size ) );
        allocationSizes.put( IdType.RELATIONSHIP_TYPE_TOKEN_NAME, config.get( relationship_type_token_name_id_allocation_size ) );
        allocationSizes.put( IdType.LABEL_TOKEN, config.get( label_token_id_allocation_size ) );
        allocationSizes.put( IdType.LABEL_TOKEN_NAME, config.get( label_token_name_id_allocation_size ) );
        allocationSizes.put( IdType.NEOSTORE_BLOCK, config.get( neostore_block_id_allocation_size ) );
        allocationSizes.put( IdType.SCHEMA, config.get( schema_id_allocation_size ) );
        allocationSizes.put( IdType.NODE_LABELS, config.get( node_labels_id_allocation_size ) );
        allocationSizes.put( IdType.RELATIONSHIP_GROUP, config.get( relationship_group_id_allocation_size ) );
        return allocationSizes;
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
                                     MemberId myself, LeaderLocator leaderLocator, long leaderLockTokenTimeout,
                                     ReplicatedLockTokenStateMachine lockTokenStateMachine )
    {
        Locks localLocks = CommunityEditionModule.createLockManager( config, logging );

        return new LeaderOnlyLockManager( myself, replicator, leaderLocator, localLocks, leaderLockTokenTimeout,
                lockTokenStateMachine );
    }
}
