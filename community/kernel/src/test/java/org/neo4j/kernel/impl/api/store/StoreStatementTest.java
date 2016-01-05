/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api.store;

import org.junit.Test;

import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class StoreStatementTest
{
    @Test
    public void shouldCloseOpenedLabelScanReader() throws Exception
    {
        // given
        LabelScanStore scanStore = mock( LabelScanStore.class );
        LabelScanReader scanReader = mock( LabelScanReader.class );

        when( scanStore.newReader() ).thenReturn( scanReader );
        StoreStatement statement = new StoreStatement(
                mock( NeoStores.class ), LockService.NO_LOCK_SERVICE,
                mock( IndexReaderFactory.class ), scanStore::newReader );

        // when
        LabelScanReader actualReader = statement.getLabelScanReader();

        // then
        assertEquals( scanReader, actualReader );

        // when
        statement.close();

        // then
        verify( scanStore ).newReader();
        verifyNoMoreInteractions( scanStore );

        verify( scanReader ).close();
        verifyNoMoreInteractions( scanReader );
    }
}
