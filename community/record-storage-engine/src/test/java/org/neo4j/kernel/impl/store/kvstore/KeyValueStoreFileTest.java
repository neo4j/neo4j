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
package org.neo4j.kernel.impl.store.kvstore;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.io.pagecache.PageCursor;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KeyValueStoreFileTest
{
    @Test
    void shouldFailAfterEnoughAttempts() throws IOException
    {
        WritableBuffer buffer = mock( WritableBuffer.class );
        PageCursor cursor = mock( PageCursor.class );
        when( cursor.shouldRetry() ).thenReturn( true );
        when( cursor.getCurrentFile() ).thenReturn( new File( "foo/bar.a" ) );

        assertThrows( UnderlyingStorageException.class, () -> KeyValueStoreFile.readKeyValuePair( cursor, 42, buffer, buffer ) );
    }
}
