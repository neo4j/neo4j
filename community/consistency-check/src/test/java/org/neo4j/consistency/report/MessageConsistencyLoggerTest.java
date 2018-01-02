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
package org.neo4j.consistency.report;

import org.junit.Test;

import org.neo4j.consistency.RecordType;
import org.neo4j.helpers.Strings;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.logging.AssertableLogProvider;

public class MessageConsistencyLoggerTest
{
    private static final AssertableLogProvider.LogMatcherBuilder INLOG = AssertableLogProvider.inLog( MessageConsistencyLoggerTest.class );
    // given
    private final InconsistencyMessageLogger logger;
    private final AssertableLogProvider logProvider;

    {
        logProvider = new AssertableLogProvider();
        logger = new InconsistencyMessageLogger( logProvider.getLog( getClass() ) );
    }

    @Test
    public void shouldFormatErrorForRecord() throws Exception
    {
        // when
        logger.error( RecordType.NEO_STORE, new NeoStoreRecord(), "sample message", 1, 2 );

        // then
        logProvider.assertExactly(
                INLOG.error( join( "sample message", neoStoreRecord( true, -1 ), "Inconsistent with: 1 2" ) )
        );
    }

    private String neoStoreRecord( boolean used, long nextProp )
    {
        NeoStoreRecord record = new NeoStoreRecord();
        record.setInUse( used );
        record.setNextProp( nextProp );
        return record.toString();
    }

    @Test
    public void shouldFlattenAMultiLineMessageToASingleLine() throws Exception
    {
        // when
        logger.error( RecordType.NEO_STORE, new NeoStoreRecord(), "multiple\n line\r\n message", 1, 2 );

        // then
        logProvider.assertExactly(
                INLOG.error( join( "multiple line message", neoStoreRecord( true, -1 ), "Inconsistent with: 1 2" ) )
        );
    }

    @Test
    public void shouldFormatWarningForRecord() throws Exception
    {
        // when
        logger.warning( RecordType.NEO_STORE, new NeoStoreRecord(), "sample message", 1, 2 );

        // then
        logProvider.assertExactly(
                INLOG.warn( join( "sample message", neoStoreRecord( true, -1 ), "Inconsistent with: 1 2" ) )
        );
    }

    @Test
    public void shouldFormatLogForChangedRecord() throws Exception
    {
        // when
        logger.error( RecordType.NEO_STORE, new NeoStoreRecord(), new NeoStoreRecord(), "sample message", 1, 2 );

        // then
        logProvider.assertExactly(
                INLOG.error( join( "sample message",
                        "- " + neoStoreRecord( true, -1 ),
                        "+ " + neoStoreRecord( true, -1 ),
                        "Inconsistent with: 1 2" ) )
        );
    }

    private String join( String firstLine, String... lines )
    {
        StringBuilder expected = new StringBuilder( firstLine );
        for ( String line : lines )
        {
            expected.append( System.lineSeparator() ).append( Strings.TAB ).append( line );
        }
        return expected.toString();
    }
}
