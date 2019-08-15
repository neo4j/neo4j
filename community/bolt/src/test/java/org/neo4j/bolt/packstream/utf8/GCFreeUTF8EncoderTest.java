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
package org.neo4j.bolt.packstream.utf8;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class GCFreeUTF8EncoderTest
{
    @Test
    void shouldThrowErrorWhenStringIsUnpackable() throws Throwable
    {
        // Given
        String invalidSurrogatePair = "\u0020\ude00";

        var encoder = UTF8Encoder.EncoderLoader.ENCODER_LOADER.fastestAvailableEncoder();
        assumeTrue( encoder instanceof GCFreeUTF8Encoder );

        var error = assertThrows( AssertionError.class, () -> encoder.encode( invalidSurrogatePair ) );
        assertThat( error.getMessage(), containsString( "Failure when converting to UTF-8." ) );
    }
}
