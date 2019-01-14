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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Test;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

public class StringSchemaKeyTest
{
    @Test
    public void shouldReuseByteArrayForFairlySimilarSizedKeys()
    {
        // given
        StringSchemaKey key = new StringSchemaKey();
        key.setBytesLength( 20 );
        byte[] first = key.bytes;

        // when
        key.setBytesLength( 25 );
        byte[] second = key.bytes;

        // then
        assertSame( first, second );
        assertThat( first.length, greaterThanOrEqualTo( 25 ) );
    }

    @Test
    public void shouldCreateNewByteArrayForVastlyDifferentKeySizes()
    {
        // given
        StringSchemaKey key = new StringSchemaKey();
        key.setBytesLength( 20 );
        byte[] first = key.bytes;

        // when
        key.setBytesLength( 100 );
        byte[] second = key.bytes;

        // then
        assertNotSame( first, second );
        assertThat( first.length, greaterThanOrEqualTo( 20 ) );
        assertThat( second.length, greaterThanOrEqualTo( 100 ) );
    }

    @Test
    public void shouldDereferenceByteArrayWhenMaterializingValue()
    {
        // given
        StringSchemaKey key = new StringSchemaKey();
        key.setBytesLength( 20 );
        byte[] first = key.bytes;

        // when
        key.asValue();
        key.setBytesLength( 25 );
        byte[] second = key.bytes;

        // then
        assertNotSame( first, second );
    }
}
