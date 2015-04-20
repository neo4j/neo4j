/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.StringWriter;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import org.neo4j.consistency.RecordType;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.junit.Assert.assertThat;

import static org.neo4j.consistency.report.InconsistencyMessageLogger.LINE_SEPARATOR;
import static org.neo4j.consistency.report.InconsistencyMessageLogger.TAB;

public class MessageConsistencyLoggerTest
{
    // given
    private final InconsistencyMessageLogger logger;
    private final StringWriter writer;

    {
        writer = new StringWriter();
        logger = new InconsistencyMessageLogger( StringLogger.wrap( writer ) );
    }

    @Test
    public void shouldFormatErrorForRecord() throws Exception
    {
        // when
        logger.error( RecordType.NEO_STORE, new NeoStoreRecord(), "sample message", 1, 2 );

        // then
        assertTextEquals( "ERROR: sample message",
                          neoStoreRecord( true, -1 ),
                          "Inconsistent with: 1 2" );
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
        assertTextEquals( "ERROR: multiple line message",
                neoStoreRecord( true, -1 ),
                "Inconsistent with: 1 2" );
    }

    @Test
    public void shouldFormatWarningForRecord() throws Exception
    {
        // when
        logger.warning( RecordType.NEO_STORE, new NeoStoreRecord(), "sample message", 1, 2 );

        // then
        assertTextEquals( "WARNING: sample message",
                          neoStoreRecord( true, -1 ),
                          "Inconsistent with: 1 2" );
    }

    @Test
    public void shouldFormatErrorForChangedRecord() throws Exception
    {
        // when
        logger.error( RecordType.NEO_STORE, new NeoStoreRecord(), new NeoStoreRecord(), "sample message", 1, 2 );

        // then
        assertTextEquals( "ERROR: sample message",
                          "- " + neoStoreRecord( true, -1 ),
                          "+ " + neoStoreRecord( true, -1 ),
                          "Inconsistent with: 1 2" );
    }

    @Test
    public void shouldFormatWarningForChangedRecord() throws Exception
    {
        // when
        logger.warning( RecordType.NEO_STORE, new NeoStoreRecord(), new NeoStoreRecord(), "sample message", 1, 2 );

        // then
        assertTextEquals( "WARNING: sample message",
                "- " + neoStoreRecord( true, -1 ),
                "+ " + neoStoreRecord( true, -1 ),
                          "Inconsistent with: 1 2" );
    }

    private void assertTextEquals( String firstLine, String... lines )
    {
        StringBuilder expected = new StringBuilder( firstLine );
        for ( String line : lines )
        {
            expected.append( LINE_SEPARATOR ).append( TAB ).append( line );
        }
        assertThat( writer.toString(), endsWith( expected.append( LINE_SEPARATOR ).toString() ) );
    }

    private static Matcher<String> endsWith( final String suffix )
    {
        return new TypeSafeMatcher<String>()
        {
            @Override
            public boolean matchesSafely( String item )
            {
                return item.endsWith( suffix );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "String ending with " ).appendValue( suffix );
            }
        };
    }
}
