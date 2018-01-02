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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Test;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReaderTest
{
    private final FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
    private final StoreChannel channel = mock( StoreChannel.class );
    private final File file = mock( File.class );

    @Test
    public void shouldCloseChannelOnClose() throws Exception
    {
        // given
        when( fsa.open( file, OpenMode.READ ) ).thenReturn( channel );
        Reader reader = new Reader( fsa, file, 0 );

        // when
        reader.close();

        // then
        verify( channel ).close();
    }

    @Test
    public void shouldUpdateTimeStamp() throws Exception
    {
        // given
        Reader reader = new Reader( fsa, file, 0 );

        // when
        int expected = 123;
        reader.setTimeStamp( expected );

        // then
        assertEquals( expected, reader.getTimeStamp() );
    }
}
