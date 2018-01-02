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
package org.neo4j.backup;

import org.junit.Test;

import org.neo4j.com.RequestContext;
import org.neo4j.com.storecopy.StoreCopyServer;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.function.Supplier;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackupImplTest
{
    @Test
    public void flushStoreFilesWithCorrectCheckpointTriggerName()
    {
        StoreCopyServer storeCopyServer = mock( StoreCopyServer.class );
        when( storeCopyServer.flushStoresAndStreamStoreFiles( anyString(), any( StoreWriter.class ), anyBoolean() ) )
                .thenReturn( RequestContext.EMPTY );

        BackupImpl backup = new BackupImpl( storeCopyServer, new Monitors(), mock( LogicalTransactionStore.class ),
                mock( TransactionIdStore.class ), mock( LogFileInformation.class ), defaultStoreIdSupplier(),
                NullLogProvider.getInstance() );

        backup.fullBackup( mock( StoreWriter.class ), false ).close();

        verify( storeCopyServer ).flushStoresAndStreamStoreFiles(
                eq( BackupImpl.FULL_BACKUP_CHECKPOINT_TRIGGER ), any( StoreWriter.class ), eq( false ) );
    }

    private static Supplier<StoreId> defaultStoreIdSupplier()
    {
        return new Supplier<StoreId>()
        {
            @Override
            public StoreId get()
            {
                return StoreId.DEFAULT;
            }
        };
    }
}
