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
package org.neo4j.kernel.ha.cluster;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.checkpoint.TriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultMasterImplSPITest
{
    @Test
    public void flushStoreFilesWithCorrectCheckpointTriggerName() throws IOException
    {
        CheckPointer checkPointer = mock( CheckPointer.class );

        NeoStoreDataSource dataSource = mock( NeoStoreDataSource.class );
        when( dataSource.listStoreFiles( anyBoolean() ) ).thenReturn( Iterators.emptyResourceIterator() );

        DefaultMasterImplSPI master = new DefaultMasterImplSPI( mock( GraphDatabaseAPI.class, RETURNS_MOCKS ),
                mock( FileSystemAbstraction.class ), new Monitors(), mock( LabelTokenHolder.class ),
                mock( PropertyKeyTokenHolder.class ), mock( RelationshipTypeTokenHolder.class ),
                mock( IdGeneratorFactory.class ), mock( TransactionCommitProcess.class ), checkPointer,
                mock( TransactionIdStore.class ), mock( LogicalTransactionStore.class ),
                dataSource, mock( PageCache.class ), new StoreCopyCheckPointMutex(), NullLogProvider.getInstance() );

        master.flushStoresAndStreamStoreFiles( mock( StoreWriter.class ) );

        TriggerInfo expectedTriggerInfo = new SimpleTriggerInfo( DefaultMasterImplSPI.STORE_COPY_CHECKPOINT_TRIGGER );
        verify( checkPointer ).tryCheckPoint( expectedTriggerInfo );
    }
}
