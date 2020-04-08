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
package org.neo4j.consistency.report;

import org.junit.jupiter.api.Test;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.store.synthetic.TokenScanDocument;
import org.neo4j.internal.helpers.Strings;
import org.neo4j.internal.index.label.EntityTokenRange;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssert;

import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

class MessageConsistencyLoggerTest
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final InconsistencyMessageLogger logger = new InconsistencyMessageLogger( logProvider.getLog( getClass() ) );
    private final LogAssert logMatcher = assertThat( logProvider ).forClass( MessageConsistencyLoggerTest.class );

    @Test
    void shouldFormatErrorForRecord()
    {
        // when
        logger.error( RecordType.NEO_STORE, new NeoStoreRecord(), "sample message", 1, 2 );

        // then
        logMatcher.forLevel( ERROR )
                .containsMessages( join( "sample message", neoStoreRecord( true, -1 ), "Inconsistent with: 1 2" ) );
    }

    @Test
    void shouldFlattenAMultiLineMessageToASingleLine()
    {
        // when
        logger.error( RecordType.NEO_STORE, new NeoStoreRecord(), "multiple\n line\r\n message", 1, 2 );

        // then
        logMatcher.forLevel( ERROR )
                .containsMessages( join( "multiple line message", neoStoreRecord( true, -1 ), "Inconsistent with: 1 2" ) );
    }

    @Test
    void shouldFormatWarningForRecord()
    {
        // when
        logger.warning( RecordType.NEO_STORE, new NeoStoreRecord(), "sample message", 1, 2 );

        // then
        logMatcher.forLevel( WARN )
                .containsMessages( join( "sample message", neoStoreRecord( true, -1 ), "Inconsistent with: 1 2" ) );
    }

    @Test
    void shouldFormatLogForChangedRecord()
    {
        // when
        logger.error( RecordType.NEO_STORE, new NeoStoreRecord(), new NeoStoreRecord(), "sample message", 1, 2 );

        // then
        logMatcher.forLevel( ERROR )
                .containsMessages( join( "sample message",
                        "- " + neoStoreRecord( true, -1 ),
                        "+ " + neoStoreRecord( true, -1 ),
                        "Inconsistent with: 1 2" ) );
    }

    @Test
    void shouldAdaptLogMessageToEntityTokenRangeTypeNode()
    {
        // when
        logger.error( RecordType.LABEL_SCAN_DOCUMENT, new TokenScanDocument( new EntityTokenRange( 0, new long[0][], NODE ) ),
                "Some label index error", new NodeRecord( 1 ) );

        // then
        logMatcher.containsMessages( "NodeLabelRange" );
    }

    @Test
    void shouldAdaptLogMessageToEntityTokenRangeTypeRelationship()
    {
        // when
        logger.error( RecordType.RELATIONSHIP_TYPE_SCAN_DOCUMENT, new TokenScanDocument( new EntityTokenRange( 0, new long[0][], RELATIONSHIP ) ),
                "Some relationship type error", new RelationshipRecord( 1 ) );

        // then
        logMatcher.containsMessages( "RelationshipTypeRange" );
    }

    private static String join( String firstLine, String... lines )
    {
        StringBuilder expected = new StringBuilder( firstLine );
        for ( String line : lines )
        {
            expected.append( System.lineSeparator() ).append( Strings.TAB ).append( line );
        }
        return expected.toString();
    }

    private static String neoStoreRecord( boolean used, long nextProp )
    {
        NeoStoreRecord record = new NeoStoreRecord();
        record.setInUse( used );
        record.setNextProp( nextProp );
        return record.toString();
    }
}
