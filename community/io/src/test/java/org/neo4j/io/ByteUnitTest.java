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
package org.neo4j.io;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ByteUnitTest
{
    @Test
    public void convertZero() throws Exception
    {
        assertThat( ByteUnit.Byte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.KibiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.MebiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.GibiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.TebiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.PebiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.ExbiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toExbiBytes( 0 ), is( 0L ) );
    }

    @Test
    public void convertOneToEIC() throws Exception
    {
        assertThat( ByteUnit.KibiByte.toBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.KibiByte.toKibiBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.KibiByte.toMebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toGibiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.MebiByte.toBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.MebiByte.toKibiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.MebiByte.toMebiBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.MebiByte.toGibiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.GibiByte.toBytes( 1 ), is( 1073741824L ) );
        assertThat( ByteUnit.GibiByte.toKibiBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.GibiByte.toMebiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.GibiByte.toGibiBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.GibiByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.TebiByte.toBytes( 1 ), is( 1099511627776L ) );
        assertThat( ByteUnit.TebiByte.toKibiBytes( 1 ), is( 1073741824L ) );
        assertThat( ByteUnit.TebiByte.toMebiBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.TebiByte.toGibiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.TebiByte.toTebiBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.TebiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.PebiByte.toBytes( 1 ), is( 1125899906842624L ) );
        assertThat( ByteUnit.PebiByte.toKibiBytes( 1 ), is( 1099511627776L ) );
        assertThat( ByteUnit.PebiByte.toMebiBytes( 1 ), is( 1073741824L ) );
        assertThat( ByteUnit.PebiByte.toGibiBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.PebiByte.toTebiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.PebiByte.toPebiBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.PebiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.ExbiByte.toBytes( 1 ), is( 1152921504606846976L ) );
        assertThat( ByteUnit.ExbiByte.toKibiBytes( 1 ), is( 1125899906842624L ) );
        assertThat( ByteUnit.ExbiByte.toMebiBytes( 1 ), is( 1099511627776L ) );
        assertThat( ByteUnit.ExbiByte.toGibiBytes( 1 ), is( 1073741824L ) );
        assertThat( ByteUnit.ExbiByte.toTebiBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.ExbiByte.toPebiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.ExbiByte.toExbiBytes( 1 ), is( 1L ) );
    }

    @Test
    public void unitsAsBytes() throws Exception
    {
        assertThat( ByteUnit.bytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.kibiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.mebiBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.gibiBytes( 1 ), is( 1073741824L ) );
        assertThat( ByteUnit.tebiBytes( 1 ), is( 1099511627776L ) );
        assertThat( ByteUnit.pebiBytes( 1 ), is( 1125899906842624L ) );
        assertThat( ByteUnit.exbiBytes( 1 ), is( 1152921504606846976L ) );
    }
}
