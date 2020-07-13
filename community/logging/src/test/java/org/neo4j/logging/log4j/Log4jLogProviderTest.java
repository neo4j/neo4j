/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.logging.log4j;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import org.neo4j.logging.Level;
import org.neo4j.logging.Log;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.logging.log4j.LogConfigTest.DATE_PATTERN;

class Log4jLogProviderTest
{
    @Test
    void getLogShouldReturnLogWithCorrectCategory()
    {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();

        Log4jLogProvider logProvider = new Log4jLogProvider( outContent );

        Log log = logProvider.getLog( "stringAsCategory" );
        log.info( "testMessage" );

        Log log2 = logProvider.getLog( Log4jLog.class );
        log2.info( "testMessage2" );

        assertThat( outContent.toString() ).matches( format( DATE_PATTERN + " %-5s \\[stringAsCategory\\] testMessage%n" +
                                                             DATE_PATTERN + " %-5s \\[o.n.l.l.Log4jLog\\] testMessage2%n", Level.INFO, Level.INFO ) );
    }
}
