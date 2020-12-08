/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.Collection;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.recordstorage.LockVerificationMonitor.NeoStoresLoader;
import org.neo4j.internal.recordstorage.LockVerificationMonitor.StoreLoader;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.lock.ResourceType;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static org.neo4j.internal.recordstorage.LockVerificationMonitor.assertRecordsEquals;
import static org.neo4j.lock.LockType.EXCLUSIVE;
import static org.neo4j.lock.ResourceTypes.NODE;
import static org.neo4j.lock.ResourceTypes.NODE_RELATIONSHIP_GROUP_DELETE;
import static org.neo4j.lock.ResourceTypes.RELATIONSHIP_GROUP;

/**
 * Merely a helper during development to ensure that commands generated are sufficiently locked, now that we're experimenting with
 * more fine-granular locking.
 */
public interface CommandLockVerification
{
    CommandLockVerification IGNORE = commands -> {};

    void verifySufficientlyLocked( Collection<StorageCommand> commands );

    class RealChecker implements CommandLockVerification
    {
        private final ResourceLocker locks;
        private final ReadableTransactionState txState;
        private final StoreLoader loader;

        RealChecker( ResourceLocker locks, ReadableTransactionState txState, NeoStores neoStores )
        {
            this.locks = locks;
            this.txState = txState;
            this.loader = new NeoStoresLoader( neoStores );
        }

        @Override
        public void verifySufficientlyLocked( Collection<StorageCommand> commands )
        {
            commands.forEach( this::verifySufficientlyLocked );
        }

        private void verifySufficientlyLocked( StorageCommand command )
        {
            if ( command instanceof Command.NodeCommand )
            {
                verifyNodeSufficientlyLocked( (Command.NodeCommand) command );
            }
            else if ( command instanceof Command.RelationshipCommand )
            {
                verifyRelationshipSufficientlyLocked( (Command.RelationshipCommand) command );
            }
            else if ( command instanceof Command.RelationshipGroupCommand )
            {
                verifyRelationshipGroupSufficientlyLocked( (Command.RelationshipGroupCommand) command );
            }
        }

        private void verifyNodeSufficientlyLocked( Command.NodeCommand command )
        {
            long id = command.getKey();
            if ( !txState.nodeIsAddedInThisTx( id ) )
            {
                assertLocked( id, NODE, EXCLUSIVE, command.after );
            }
            if ( txState.nodeIsDeletedInThisTx( id ) )
            {
                assertLocked( id, NODE_RELATIONSHIP_GROUP_DELETE, EXCLUSIVE, command.after );
            }
        }

        private void verifyRelationshipSufficientlyLocked( Command.RelationshipCommand command )
        {
            LockVerificationMonitor.checkRelationship( txState, locks, loader, command.after );

            if ( command.before.inUse() )
            {
                assertRecordsEquals( command.before, loader::loadRelationship );
            }
        }

        private void verifyRelationshipGroupSufficientlyLocked( Command.RelationshipGroupCommand command )
        {
            long node = command.after.getOwningNode();
            if ( !txState.nodeIsAddedInThisTx( node ) )
            {
                assertLocked( node, RELATIONSHIP_GROUP, EXCLUSIVE, command.after );
            }

            boolean deleted = !command.after.inUse();
            if ( deleted )
            {
                assertLocked( node, NODE_RELATIONSHIP_GROUP_DELETE, EXCLUSIVE, command.after );
            }

            if ( command.before.inUse() )
            {
                assertRecordsEquals( command.before, loader::loadRelationshipGroup );
            }
        }

        private void assertLocked( long id, ResourceType resource, LockType type, AbstractBaseRecord record )
        {
            LockVerificationMonitor.assertLocked( locks, id, resource, type, record );
        }
    }

    interface Factory
    {
        Factory IGNORE = ( locker, txState, storageReader ) -> CommandLockVerification.IGNORE;

        CommandLockVerification create( ResourceLocker locker, ReadableTransactionState txState, NeoStores neoStores );

        class RealFactory implements Factory
        {
            private final Config config;

            RealFactory( Config config )
            {
                this.config = config;
            }

            @Override
            public CommandLockVerification create( ResourceLocker locker, ReadableTransactionState txState, NeoStores neoStores )
            {
                boolean enabled = config.get( GraphDatabaseInternalSettings.additional_lock_verification );
                return enabled ? new RealChecker( locker, txState, neoStores ) : CommandLockVerification.IGNORE;
            }
        }
    }
}
