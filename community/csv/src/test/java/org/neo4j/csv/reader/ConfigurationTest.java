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

package org.neo4j.csv.reader;

import org.junit.jupiter.api.Test;

import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;
import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigurationTest
{
    @Test
    void toBuilderThenBuildDefault()
    {
        final var before = Configuration.newBuilder().build();
        final var after = before.toBuilder().build();

        assertEquals( reflectionToString( before, SHORT_PREFIX_STYLE ), reflectionToString( after, SHORT_PREFIX_STYLE ) );
    }

    @Test
    void toBuilderThenBuildNonDefault()
    {
        final var before = Configuration.newBuilder()
                .withDelimiter( '1' )
                .withArrayDelimiter( '2' )
                .withQuotationCharacter( '3' )
                .withBufferSize( 100500 )
                .withLegacyStyleQuoting( true )
                .withEmptyQuotedStringsAsNull( true )
                .withMultilineFields( true )
                .withTrimStrings( true )
                .build();
        final var after = before.toBuilder().build();

        assertEquals( reflectionToString( before, SHORT_PREFIX_STYLE), reflectionToString( after, SHORT_PREFIX_STYLE ) );
    }
}
