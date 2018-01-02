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
package org.neo4j.logging;

import org.junit.Test;
import org.neo4j.function.Consumer;

public class DuplicatingLogTest
{
    @Test
    public void shouldOutputToMultipleLogs()
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log1 = logProvider.getLog( "log 1" );
        Log log2 = logProvider.getLog( "log 2" );

        DuplicatingLog log = new DuplicatingLog( log1, log2 );

        // When
        log.info( "When the going gets weird" );

        // Then
        logProvider.assertExactly(
                AssertableLogProvider.inLog( "log 1" ).info( "When the going gets weird" ),
                AssertableLogProvider.inLog( "log 2" ).info( "When the going gets weird" )
        );
    }

    @Test
    public void shouldBulkOutputToMultipleLogs()
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log1 = logProvider.getLog( "log 1" );
        Log log2 = logProvider.getLog( "log 2" );

        DuplicatingLog log = new DuplicatingLog( log1, log2 );

        // When
        log.bulk( new Consumer<Log>()
        {
            @Override
            public void accept( Log bulkLog )
            {
                bulkLog.info( "When the going gets weird" );
            }
        } );

        // Then
        logProvider.assertExactly(
                AssertableLogProvider.inLog( "log 1" ).info( "When the going gets weird" ),
                AssertableLogProvider.inLog( "log 2" ).info( "When the going gets weird" )
        );
    }

    @Test
    public void shouldRemoveLogFromDuplication()
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log1 = logProvider.getLog( "log 1" );
        Log log2 = logProvider.getLog( "log 2" );

        DuplicatingLog log = new DuplicatingLog( log1, log2 );

        // When
        log.info( "When the going gets weird" );
        log.remove( log1 );
        log.info( "The weird turn pro" );

        // Then
        logProvider.assertExactly(
                AssertableLogProvider.inLog( "log 1" ).info( "When the going gets weird" ),
                AssertableLogProvider.inLog( "log 2" ).info( "When the going gets weird" ),
                AssertableLogProvider.inLog( "log 2" ).info( "The weird turn pro" )
        );
    }

    @Test
    public void shouldRemoveLoggersFromDuplication()
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log1 = logProvider.getLog( "log 1" );
        Log log2 = logProvider.getLog( "log 2" );

        DuplicatingLog log = new DuplicatingLog( log1, log2 );
        Logger logger = log.infoLogger();

        // When
        logger.log( "When the going gets weird" );
        log.remove( log1 );
        logger.log( "The weird turn pro" );

        // Then
        logProvider.assertExactly(
                AssertableLogProvider.inLog( "log 1" ).info( "When the going gets weird" ),
                AssertableLogProvider.inLog( "log 2" ).info( "When the going gets weird" ),
                AssertableLogProvider.inLog( "log 2" ).info( "The weird turn pro" )
        );
    }
}
