/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.io;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import org.neo4j.test.matchers.NestedThrowableMatcher;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@RunWith( MockitoJUnitRunner.class )
public class IOUtilsTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private AutoCloseable faultyClosable;
    @Mock
    private AutoCloseable goodClosable1;
    @Mock
    private AutoCloseable goodClosable2;

    @Test
    public void closeAllSilently() throws Exception
    {
        IOUtils.closeAllSilently( goodClosable1, faultyClosable, goodClosable2 );

        verify( goodClosable1 ).close();
        verify( goodClosable2 ).close();
        verify( faultyClosable ).close();
    }

    @Test
    public void closeAllAndRethrowException() throws Exception
    {
        doThrow( new IOException( "Faulty closable" ) ).when( faultyClosable ).close();

        expectedException.expect( IOException.class );
        expectedException.expectMessage( "Exception closing multiple resources" );
        expectedException.expect( new NestedThrowableMatcher( IOException.class ) );

        IOUtils.closeAll( goodClosable1, faultyClosable, goodClosable2 );
    }

}
