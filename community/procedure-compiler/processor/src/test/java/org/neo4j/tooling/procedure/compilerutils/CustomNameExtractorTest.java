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
package org.neo4j.tooling.procedure.compilerutils;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CustomNameExtractorTest
{

    @Test
    public void favours_name_over_value()
    {
        assertThat(CustomNameExtractor.getName( () -> "name", () -> "value" )).contains( "name" );
        assertThat(CustomNameExtractor.getName( () -> "name", () -> "" )).contains( "name" );
        assertThat(CustomNameExtractor.getName( () -> "name", () -> "  " )).contains( "name" );
        assertThat(CustomNameExtractor.getName( () -> "   name  ", () -> "  " )).contains( "name" );
    }

    @Test
    public void returns_value_if_trimmed_name_is_empty()
    {
        assertThat(CustomNameExtractor.getName( () -> "", () -> "value" )).contains( "value" );
        assertThat(CustomNameExtractor.getName( () -> "   ", () -> "value" )).contains( "value" );
        assertThat(CustomNameExtractor.getName( () -> "   ", () -> "   value  " )).contains( "value" );
    }

    @Test
    public void returns_nothing_if_none_defined()
    {
        assertThat(CustomNameExtractor.getName( () -> "", () -> "" )).isEmpty();
        assertThat(CustomNameExtractor.getName( () -> "   ", () -> "" )).isEmpty();
        assertThat(CustomNameExtractor.getName( () -> "", () -> "   " )).isEmpty();
        assertThat(CustomNameExtractor.getName( () -> "   ", () -> "   " )).isEmpty();
    }
}
