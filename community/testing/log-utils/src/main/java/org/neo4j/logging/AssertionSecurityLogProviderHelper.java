/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.logging;

import org.assertj.core.api.Assertions;

import java.io.ByteArrayOutputStream;

import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;

public class AssertionSecurityLogProviderHelper
{
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final Log4jLogProvider logProvider;

    public AssertionSecurityLogProviderHelper()
    {
        logProvider = new Log4jLogProvider( LogConfig.createBuilder( outContent, Level.INFO )
                                                     .withFormat( FormattedLogFormat.STANDARD_FORMAT )
                                                     .withCategory( false )
                                                     .build() );
    }

    public Log4jLogProvider getLogProvider()
    {
        return logProvider;
    }

    public void assertContainsMessage( Level level, String message, Integer index )
    {
        String[] logLines = outContent.toString().split( System.lineSeparator() );
        Assertions.assertThat( logLines[index] ).contains( message );
        Assertions.assertThat( logLines[index] ).contains( level.toString() );
    }

    public void assertContainsMessage( Level level, String message )
    {
        Assertions.assertThat( outContent.toString() ).contains( message );
        Assertions.assertThat( outContent.toString() ).contains( level.toString() );
    }

    public void assertContainsMessage( Level level, String format, Object... arguments )
    {
        assertContainsMessage( level, String.format( format, arguments ) );
    }

    public void assertDoesNotContainsMessage( String message )
    {
        Assertions.assertThat( outContent.toString() ).doesNotContain( message );
    }
}
