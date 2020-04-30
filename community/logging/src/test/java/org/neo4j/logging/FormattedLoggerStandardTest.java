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
package org.neo4j.logging;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.Supplier;

import org.neo4j.function.Suppliers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;
import static org.neo4j.logging.FormattedLogTest.newFormattedLog;

public class FormattedLoggerStandardTest
{
    private static final Supplier<ZonedDateTime> DATE_TIME_SUPPLIER = () ->
            ZonedDateTime.of( 2020, 01, 02, 3, 4, 5, 678000000, ZoneOffset.UTC );
    private StringWriter writer;
    private FormattedLog log;

    @BeforeEach
    void setup()
    {
        writer = new StringWriter();
        log = newFormattedLog( writer, Level.DEBUG );
    }

    @Test
    void shouldThrowOnUnrecognizedLevel()
    {
        assertThatIllegalArgumentException()
                .isThrownBy( () -> new FormattedLoggerStandard( log, () -> null, Level.NONE, null, null, null ) )
                .withMessage( "Cannot create FormattedLogger with Level " + Level.NONE );
    }

    @ParameterizedTest
    @EnumSource( value = Level.class, mode = EXCLUDE, names = {"NONE"} )
    void shouldFormatLevelCorrectly( Level level )
    {
        FormattedLoggerStandard logger =
                new FormattedLoggerStandard( log, Suppliers.singleton( new PrintWriter( writer ) ), level, null, FormattedLoggerStandard.DATE_TIME_FORMATTER,
                                             DATE_TIME_SUPPLIER );

        logger.writeLog( new PrintWriter( writer ), "message" );

        // The level name should have be padded to 5 characters to align the messages.
        assertThat( writer.toString() ).isEqualTo( "2020-01-02 03:04:05.678+0000 %1$-5s message%n", level.name() );
    }

    @ParameterizedTest
    @EnumSource( value = Level.class, mode = EXCLUDE, names = {"NONE"} )
    void shouldFormatLevelAndCategoryCorrectly( Level level )
    {
        FormattedLoggerStandard logger =
                new FormattedLoggerStandard( log, Suppliers.singleton( new PrintWriter( writer ) ), level, "category",
                                             FormattedLoggerStandard.DATE_TIME_FORMATTER,
                                             DATE_TIME_SUPPLIER );

        logger.writeLog( new PrintWriter( writer ), "message" );

        assertThat( writer.toString() ).isEqualTo( "2020-01-02 03:04:05.678+0000 %s [category] message%n", level.name() );
    }
}
