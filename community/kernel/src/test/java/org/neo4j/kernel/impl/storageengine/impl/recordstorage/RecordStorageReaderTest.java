/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.junit.Test;

import java.util.function.Supplier;

import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.test.MockedNeoStores;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RecordStorageReaderTest
{
    @Test
    public void shouldCloseOpenedLabelScanReader()
    {
        // given
        Supplier<LabelScanReader> scanStore = mock( Supplier.class );
        LabelScanReader scanReader = mock( LabelScanReader.class );

        when( scanStore.get() ).thenReturn( scanReader );
        RecordStorageReader statement = new RecordStorageReader( null, null, null, null, MockedNeoStores.basicMockedNeoStores(), null, null,
                mock( Supplier.class ), scanStore, mock( RecordStorageCommandCreationContext.class ) );
        statement.acquire();

        // when
        LabelScanReader actualReader = statement.getLabelScanReader();

        // then
        assertEquals( scanReader, actualReader );

        // when
        statement.close();

        // then
        verify( scanStore ).get();
        verifyNoMoreInteractions( scanStore );

        verify( scanReader ).close();
        verifyNoMoreInteractions( scanReader );
    }
}
