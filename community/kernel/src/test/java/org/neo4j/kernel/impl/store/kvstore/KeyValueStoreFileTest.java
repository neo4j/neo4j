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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KeyValueStoreFileTest
{
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( Timeout.seconds( 1000 ) ).around( exception );

    @Test
    public void shouldFailAfterEnoughAttempts() throws IOException
    {
        // given
        WritableBuffer buffer = mock( WritableBuffer.class );
        PageCursor cursor = mock( PageCursor.class );
        when( cursor.shouldRetry() ).thenReturn( true );
        when( cursor.getCurrentFile() ).thenReturn( new File( "foo/bar.a" ) );

        // then
        exception.expect( UnderlyingStorageException.class );

        // when
        KeyValueStoreFile.readKeyValuePair( cursor, 42, buffer, buffer );
    }
}
