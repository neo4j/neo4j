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
package org.neo4j.kernel.ha.cluster;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.TriggerInfo;
import org.neo4j.kernel.monitoring.Monitors;

import static org.mockito.Matchers.anyBoolean;
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
        when( dataSource.listStoreFiles( anyBoolean() ) ).thenReturn( IteratorUtil.<File>emptyIterator() );

        DefaultMasterImplSPI master = new DefaultMasterImplSPI( mock( GraphDatabaseAPI.class, RETURNS_MOCKS ),
                mock( FileSystemAbstraction.class ), new Monitors(), mock( LabelTokenHolder.class ),
                mock( PropertyKeyTokenHolder.class ), mock( RelationshipTypeTokenHolder.class ),
                mock( IdGeneratorFactory.class ), mock( TransactionCommitProcess.class ), checkPointer,
                mock( TransactionIdStore.class ), mock( LogicalTransactionStore.class ),
                dataSource );

        master.flushStoresAndStreamStoreFiles( mock( StoreWriter.class ) );

        TriggerInfo expectedTriggerInfo = new SimpleTriggerInfo( DefaultMasterImplSPI.STORE_COPY_CHECKPOINT_TRIGGER );
        verify( checkPointer ).tryCheckPoint( expectedTriggerInfo );
    }
}
