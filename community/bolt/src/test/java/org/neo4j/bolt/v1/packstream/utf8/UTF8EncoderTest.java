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
package org.neo4j.bolt.v1.packstream.utf8;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class UTF8EncoderTest
{
    @Test
    public void shouldEncodeDecode()
    {
        assertEncodes( "" );
        assertEncodes( "a" );
        assertEncodes( "ä" );
        assertEncodes( "äa" );
        assertEncodes( "基本上，電腦只是處理數位。它們指定一個數位，來儲存字母或其他字元。在創造Unicode之前，" +
                "有數百種指定這些數位的編碼系統。沒有一個編碼可以包含足夠的字元，例如：單單歐洲共同體就需要好幾種不同的編碼來包括所有的語言。" +
                "即使是單一種語言，例如英語，也沒有哪一個編碼可以適用於所有的字母、標點符號，和常用的技術符號" );
        assertEncodes( new String( new byte[(int) Math.pow( 2, 18 )] ) ); // bigger than default buffer size
    }

    private void assertEncodes( String val )
    {
        assertEquals( val,  encodeDecode( val ) );
    }

    private String encodeDecode( String original )
    {
        ByteBuffer encoded = UTF8Encoder.fastestAvailableEncoder().encode( original );
        byte[] b = new byte[encoded.remaining()];
        encoded.get( b );
        return new String( b, StandardCharsets.UTF_8 );
    }
}
