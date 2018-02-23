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
package org.neo4j.server.scripting.javascript;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.server.scripting.javascript.GlobalJavascriptInitializer.Mode.SANDBOXED;
import static org.neo4j.server.scripting.javascript.GlobalJavascriptInitializer.Mode.UNSAFE;
import static org.neo4j.server.scripting.javascript.GlobalJavascriptInitializer.initialize;

public class TestGlobalJavascriptInitializer
{

    @Test
    public void shouldNotAllowChangingMode()
    {
        assertThrows( RuntimeException.class, () -> {
            // Given
            initialize( SANDBOXED );

            // When
            initialize( UNSAFE );
        } );
    }

    @Test
    public void initializingTheSameModeTwiceIsFine()
    {
        // Given
        initialize( SANDBOXED );

        // When
        initialize( SANDBOXED );

        // Then
        // no exception should have been thrown.
    }

}
