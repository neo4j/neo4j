/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index.storage.paged;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.io.pagecache.PagedFile;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class InputResourcesTest
{
    @Test
    public void shouldClosePagedFileEvenIfCloneFailsToClose() throws Exception
    {
        // Given
        PagedFile file = mock( PagedFile.class );

        InputResources.RootInputResources resources = new InputResources.RootInputResources( file );

        PagedIndexInput rootInput = new PagedIndexInput( resources, "RootInput", 0, 14, 1337 );
        new PagedIndexInput( resources.cloneResources(), "CloneInput", 0, 14, 1337 );
        FailingInput firstFailingClone =
                new FailingInput( resources.cloneResources(), "FirstFailingClone", 0, 14, 1337 );
        FailingInput secondFailingClone =
                new FailingInput( resources.cloneResources(), "SecondFailingClone", 0, 14, 1337 );

        // When
        try
        {
            rootInput.close();
            fail( "Should have thrown IOException" );
        }
        catch ( IOException e )
        {
            assertEquals( e.getMessage(), "Emulated error to close.." );
        }

        // Then
        assertEquals( firstFailingClone.closeCalls, 1 );
        assertEquals( secondFailingClone.closeCalls, 1 );
        verify( file ).close();
    }

    static class FailingInput extends PagedIndexInput
    {
        public int closeCalls;

        FailingInput( InputResources resources, String resourceDescription, long startPosition, int pageSize,
                long size ) throws IOException
        {
            super( resources, resourceDescription, startPosition, pageSize, size );
        }

        @Override
        public void close() throws IOException
        {
            closeCalls++;
            throw new IOException( "Emulated error to close.." );
        }
    }
}
