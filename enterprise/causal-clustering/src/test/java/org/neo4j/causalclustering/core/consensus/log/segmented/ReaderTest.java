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
