/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
        assertThat( ByteUnit.Byte.toKiloBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toMegaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toGigaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toTeraBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toPetaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toExaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.KiloByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toKiloBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toMegaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toGigaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toTeraBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toPetaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toExaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.KibiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toKiloBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toMegaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toGigaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toTeraBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toPetaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toExaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.MegaByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toKiloBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toMegaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toGigaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toTeraBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toPetaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toExaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.MebiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toKiloBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toMegaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toGigaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toTeraBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toPetaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toExaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.GigaByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toKiloBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toMegaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toGigaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toTeraBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toPetaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toExaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.GibiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toKiloBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toMegaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toGigaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toTeraBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toPetaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toExaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.TeraByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toKiloBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toMegaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toGigaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toTeraBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toPetaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toExaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.TebiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toKiloBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toMegaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toGigaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toTeraBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toPetaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toExaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.PetaByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PetaByte.toKiloBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PetaByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PetaByte.toMegaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PetaByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PetaByte.toGigaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PetaByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PetaByte.toTeraBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PetaByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PetaByte.toPetaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PetaByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PetaByte.toExaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PetaByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.PebiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toKiloBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toMegaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toGigaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toTeraBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toPetaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toExaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.ExaByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExaByte.toKiloBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExaByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExaByte.toMegaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExaByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExaByte.toGigaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExaByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExaByte.toTeraBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExaByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExaByte.toPetaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExaByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExaByte.toExaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExaByte.toExbiBytes( 0 ), is( 0L ) );

        assertThat( ByteUnit.ExbiByte.toBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toKiloBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toKibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toMegaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toMebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toGigaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toGibiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toTeraBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toTebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toPetaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toPebiBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toExaBytes( 0 ), is( 0L ) );
        assertThat( ByteUnit.ExbiByte.toExbiBytes( 0 ), is( 0L ) );
    }

    @Test
    public void convertOneToSI() throws Exception
    {
        assertThat( ByteUnit.Byte.toBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.Byte.toKiloBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toKibiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toMegaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toMebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toGigaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toGibiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toTeraBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toPetaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toExaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.Byte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.KiloByte.toBytes( 1 ), is( 1000L ) );
        assertThat( ByteUnit.KiloByte.toKiloBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.KiloByte.toKibiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toMegaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toMebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toGigaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toGibiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toTeraBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toPetaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toExaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KiloByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.MegaByte.toBytes( 1 ), is( 1000000L ) );
        assertThat( ByteUnit.MegaByte.toKiloBytes( 1 ), is( 1000L ) );
        assertThat( ByteUnit.MegaByte.toKibiBytes( 1 ), is( 976L ) );
        assertThat( ByteUnit.MegaByte.toMegaBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.MegaByte.toMebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toGigaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toGibiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toTeraBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toPetaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toExaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MegaByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.GigaByte.toBytes( 1 ), is( 1000000000L ) );
        assertThat( ByteUnit.GigaByte.toKiloBytes( 1 ), is( 1000000L ) );
        assertThat( ByteUnit.GigaByte.toKibiBytes( 1 ), is( 976562L ) );
        assertThat( ByteUnit.GigaByte.toMegaBytes( 1 ), is( 1000L ) );
        assertThat( ByteUnit.GigaByte.toMebiBytes( 1 ), is( 953L ) );
        assertThat( ByteUnit.GigaByte.toGigaBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.GigaByte.toGibiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toTeraBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toPetaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toExaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.GigaByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.TeraByte.toBytes( 1 ), is( 1000000000000L ) );
        assertThat( ByteUnit.TeraByte.toKiloBytes( 1 ), is( 1000000000L ) );
        assertThat( ByteUnit.TeraByte.toKibiBytes( 1 ), is( 976562500L ) );
        assertThat( ByteUnit.TeraByte.toMegaBytes( 1 ), is( 1000000L ) );
        assertThat( ByteUnit.TeraByte.toMebiBytes( 1 ), is( 953674L ) );
        assertThat( ByteUnit.TeraByte.toGigaBytes( 1 ), is( 1000L ) );
        assertThat( ByteUnit.TeraByte.toGibiBytes( 1 ), is( 931L ) );
        assertThat( ByteUnit.TeraByte.toTeraBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.TeraByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toPetaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toExaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.TeraByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.PetaByte.toBytes( 1 ), is( 1000000000000000L ) );
        assertThat( ByteUnit.PetaByte.toKiloBytes( 1 ), is( 1000000000000L ) );
        assertThat( ByteUnit.PetaByte.toKibiBytes( 1 ), is( 976562500000L ) );
        assertThat( ByteUnit.PetaByte.toMegaBytes( 1 ), is( 1000000000L ) );
        assertThat( ByteUnit.PetaByte.toMebiBytes( 1 ), is( 953674316L ) );
        assertThat( ByteUnit.PetaByte.toGigaBytes( 1 ), is( 1000000L ) );
        assertThat( ByteUnit.PetaByte.toGibiBytes( 1 ), is( 931322L ) );
        assertThat( ByteUnit.PetaByte.toTeraBytes( 1 ), is( 1000L ) );
        assertThat( ByteUnit.PetaByte.toTebiBytes( 1 ), is( 909L ) );
        assertThat( ByteUnit.PetaByte.toPetaBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.PetaByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.PetaByte.toExaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.PetaByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.ExaByte.toBytes( 1 ), is( 1000000000000000000L ) );
        assertThat( ByteUnit.ExaByte.toKiloBytes( 1 ), is( 1000000000000000L ) );
        assertThat( ByteUnit.ExaByte.toKibiBytes( 1 ), is( 976562500000000L ) );
        assertThat( ByteUnit.ExaByte.toMegaBytes( 1 ), is( 1000000000000L ) );
        assertThat( ByteUnit.ExaByte.toMebiBytes( 1 ), is( 953674316406L ) );
        assertThat( ByteUnit.ExaByte.toGigaBytes( 1 ), is( 1000000000L ) );
        assertThat( ByteUnit.ExaByte.toGibiBytes( 1 ), is( 931322574L ) );
        assertThat( ByteUnit.ExaByte.toTeraBytes( 1 ), is( 1000000L ) );
        assertThat( ByteUnit.ExaByte.toTebiBytes( 1 ), is( 909494L ) );
        assertThat( ByteUnit.ExaByte.toPetaBytes( 1 ), is( 1000L ) );
        assertThat( ByteUnit.ExaByte.toPebiBytes( 1 ), is( 888L ) );
        assertThat( ByteUnit.ExaByte.toExaBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.ExaByte.toExbiBytes( 1 ), is( 0L ) );
    }

    @Test
    public void convertOneToEIC() throws Exception
    {
        assertThat( ByteUnit.KibiByte.toBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.KibiByte.toKiloBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.KibiByte.toKibiBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.KibiByte.toMegaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toMebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toGigaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toGibiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toTeraBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toPetaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toExaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.KibiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.MebiByte.toBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.MebiByte.toKiloBytes( 1 ), is( 1048L ) );
        assertThat( ByteUnit.MebiByte.toKibiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.MebiByte.toMegaBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.MebiByte.toMebiBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.MebiByte.toGigaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toGibiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toTeraBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toPetaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toExaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.MebiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.GibiByte.toBytes( 1 ), is( 1073741824L ) );
        assertThat( ByteUnit.GibiByte.toKiloBytes( 1 ), is( 1073741L ) );
        assertThat( ByteUnit.GibiByte.toKibiBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.GibiByte.toMegaBytes( 1 ), is( 1073L ) );
        assertThat( ByteUnit.GibiByte.toMebiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.GibiByte.toGigaBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.GibiByte.toGibiBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.GibiByte.toTeraBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toTebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toPetaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toExaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.GibiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.TebiByte.toBytes( 1 ), is( 1099511627776L ) );
        assertThat( ByteUnit.TebiByte.toKiloBytes( 1 ), is( 1099511627L ) );
        assertThat( ByteUnit.TebiByte.toKibiBytes( 1 ), is( 1073741824L ) );
        assertThat( ByteUnit.TebiByte.toMegaBytes( 1 ), is( 1099511L ) );
        assertThat( ByteUnit.TebiByte.toMebiBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.TebiByte.toGigaBytes( 1 ), is( 1099L ) );
        assertThat( ByteUnit.TebiByte.toGibiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.TebiByte.toTeraBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.TebiByte.toTebiBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.TebiByte.toPetaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toPebiBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toExaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.TebiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.PebiByte.toBytes( 1 ), is( 1125899906842624L ) );
        assertThat( ByteUnit.PebiByte.toKiloBytes( 1 ), is( 1125899906842L ) );
        assertThat( ByteUnit.PebiByte.toKibiBytes( 1 ), is( 1099511627776L ) );
        assertThat( ByteUnit.PebiByte.toMegaBytes( 1 ), is( 1125899906L ) );
        assertThat( ByteUnit.PebiByte.toMebiBytes( 1 ), is( 1073741824L ) );
        assertThat( ByteUnit.PebiByte.toGigaBytes( 1 ), is( 1125899L ) );
        assertThat( ByteUnit.PebiByte.toGibiBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.PebiByte.toTeraBytes( 1 ), is( 1125L ) );
        assertThat( ByteUnit.PebiByte.toTebiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.PebiByte.toPetaBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.PebiByte.toPebiBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.PebiByte.toExaBytes( 1 ), is( 0L ) );
        assertThat( ByteUnit.PebiByte.toExbiBytes( 1 ), is( 0L ) );

        assertThat( ByteUnit.ExbiByte.toBytes( 1 ), is( 1152921504606846976L ) );
        assertThat( ByteUnit.ExbiByte.toKiloBytes( 1 ), is( 1152921504606846L ) );
        assertThat( ByteUnit.ExbiByte.toKibiBytes( 1 ), is( 1125899906842624L ) );
        assertThat( ByteUnit.ExbiByte.toMegaBytes( 1 ), is( 1152921504606L ) );
        assertThat( ByteUnit.ExbiByte.toMebiBytes( 1 ), is( 1099511627776L ) );
        assertThat( ByteUnit.ExbiByte.toGigaBytes( 1 ), is( 1152921504L ) );
        assertThat( ByteUnit.ExbiByte.toGibiBytes( 1 ), is( 1073741824L ) );
        assertThat( ByteUnit.ExbiByte.toTeraBytes( 1 ), is( 1152921L ) );
        assertThat( ByteUnit.ExbiByte.toTebiBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.ExbiByte.toPetaBytes( 1 ), is( 1152L ) );
        assertThat( ByteUnit.ExbiByte.toPebiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.ExbiByte.toExaBytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.ExbiByte.toExbiBytes( 1 ), is( 1L ) );
    }

    @Test
    public void unitsAsBytes() throws Exception
    {
        assertThat( ByteUnit.bytes( 1 ), is( 1L ) );
        assertThat( ByteUnit.kiloBytes( 1 ), is( 1000L ) );
        assertThat( ByteUnit.kibiBytes( 1 ), is( 1024L ) );
        assertThat( ByteUnit.megaBytes( 1 ), is( 1000000L ) );
        assertThat( ByteUnit.mebiBytes( 1 ), is( 1048576L ) );
        assertThat( ByteUnit.gigaBytes( 1 ), is( 1000000000L ) );
        assertThat( ByteUnit.gibiBytes( 1 ), is( 1073741824L ) );
        assertThat( ByteUnit.teraBytes( 1 ), is( 1000000000000L ) );
        assertThat( ByteUnit.tebiBytes( 1 ), is( 1099511627776L ) );
        assertThat( ByteUnit.petaBytes( 1 ), is( 1000000000000000L ) );
        assertThat( ByteUnit.pebiBytes( 1 ), is( 1125899906842624L ) );
        assertThat( ByteUnit.exaBytes( 1 ), is( 1000000000000000000L ) );
        assertThat( ByteUnit.exbiBytes( 1 ), is( 1152921504606846976L ) );
    }
}
