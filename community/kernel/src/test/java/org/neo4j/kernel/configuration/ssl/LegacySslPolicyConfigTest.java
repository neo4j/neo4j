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
package org.neo4j.kernel.configuration.ssl;

import org.junit.Test;

import java.util.stream.Stream;

import org.neo4j.configuration.ConfigValue;
import org.neo4j.kernel.configuration.Config;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig.certificates_directory;

public class LegacySslPolicyConfigTest
{
    @Test
    public void shouldBeFoundInServerDefaults()
    {
        // given
        Config serverDefaultConfig = Config.builder().withServerDefaults().build();

        // when
        Stream<ConfigValue> cvStream = serverDefaultConfig.getConfigValues().values().stream();

        // then
        assertEquals( 1, cvStream.filter( c -> c.name().equals( certificates_directory.name() ) ).count() );
    }
}
