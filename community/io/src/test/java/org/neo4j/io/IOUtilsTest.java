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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.neo4j.io.IOUtils.closeAll;

class IOUtilsTest
{
    private final AutoCloseable faultyClosable = Mockito.mock( AutoCloseable.class );
    private final AutoCloseable goodClosable1 = Mockito.mock( AutoCloseable.class );
    private final AutoCloseable goodClosable2 = Mockito.mock( AutoCloseable.class );

    @BeforeEach
    void setUp() throws Exception
    {
        doThrow( new IOException( "Faulty closable" ) ).when( faultyClosable ).close();
    }

    @Test
    void closeAllSilently() throws Exception
    {
        IOUtils.closeAllSilently( goodClosable1, faultyClosable, goodClosable2 );

        verify( goodClosable1 ).close();
        verify( goodClosable2 ).close();
        verify( faultyClosable ).close();
    }

    @Test
    void closeAllAndRethrowException()
    {
        final var e = assertThrows( IOException.class, () -> closeAll( goodClosable1, faultyClosable, goodClosable2 ) );
        assertThat( e.getMessage(), is( "Exception closing multiple resources." ) );
        assertThat( e.getCause(), isA( IOException.class ) );
    }

    @Test
    void closeMustIgnoreNullResources() throws Exception
    {
        AutoCloseable a = () -> {};
        AutoCloseable b = null;
        AutoCloseable c = () -> {};
        IOUtils.close( IOException::new, a, b, c );
    }
}
