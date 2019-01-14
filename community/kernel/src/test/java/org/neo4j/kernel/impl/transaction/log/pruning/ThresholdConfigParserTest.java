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
package org.neo4j.kernel.impl.transaction.log.pruning;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.ThresholdConfigValue;
import static org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.ThresholdConfigValue.KEEP_LAST_FILE;
import static org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.ThresholdConfigValue.NO_PRUNING;
import static org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.parse;

class ThresholdConfigParserTest
{
    @Test
    void parseTrue()
    {
        ThresholdConfigValue configValue = parse( "true" );
        assertEquals( NO_PRUNING, configValue );
    }

    @Test
    void parseFalse()
    {
        ThresholdConfigValue configValue = parse( "false" );
        assertEquals( KEEP_LAST_FILE, configValue );
    }

    @Test
    void parseGarbage()
    {
        assertThrows( IllegalArgumentException.class, () -> parse( "davide" ) );
    }
}
