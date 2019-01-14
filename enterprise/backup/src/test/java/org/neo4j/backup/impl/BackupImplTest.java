/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.backup.impl;

import org.junit.Test;

import java.util.function.Supplier;

import org.neo4j.com.RequestContext;
import org.neo4j.com.storecopy.StoreCopyServer;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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

        BackupImpl backup = new BackupImpl( storeCopyServer, mock( LogicalTransactionStore.class ),
                mock( TransactionIdStore.class ), mock( LogFileInformation.class ), defaultStoreIdSupplier(),
                NullLogProvider.getInstance() );

        backup.fullBackup( mock( StoreWriter.class ), false ).close();

        verify( storeCopyServer ).flushStoresAndStreamStoreFiles(
                eq( BackupImpl.FULL_BACKUP_CHECKPOINT_TRIGGER ), any( StoreWriter.class ), eq( false ) );
    }

    private static Supplier<StoreId> defaultStoreIdSupplier()
    {
        return () -> StoreId.DEFAULT;
    }
}
