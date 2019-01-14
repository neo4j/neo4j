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
package org.neo4j.consistency.report;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.report.ConsistencyReporter.ReportHandler;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.consistency.report.ConsistencyReporter.NO_MONITOR;

class PendingReferenceCheckTest
{
    private PendingReferenceCheck<PropertyRecord> referenceCheck;

    @SuppressWarnings( "unchecked" )
    @BeforeEach
    void setUp()
    {
        RecordAccess records = mock( RecordAccess.class );
        ReportHandler handler = new ReportHandler( mock( InconsistencyReport.class ), mock( ConsistencyReporter.ProxyFactory.class ), RecordType.PROPERTY,
                records, new PropertyRecord( 0 ), NO_MONITOR );
        this.referenceCheck = new PendingReferenceCheck<>( handler, mock( ComparativeRecordChecker.class ) );
    }

    @Test
    void shouldAllowSkipAfterSkip()
    {
        // given
        referenceCheck.skip();
        // when
        referenceCheck.skip();
    }

    @Test
    void shouldAllowSkipAfterCheckReference()
    {
        // given
        referenceCheck.checkReference( new PropertyRecord( 0 ), null );
        // when
        referenceCheck.skip();
    }

    @Test
    void shouldAllowSkipAfterCheckDiffReference()
    {
        // given
        referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null );
        // when
        referenceCheck.skip();
    }

    @Test
    void shouldNotAllowCheckReferenceAfterSkip()
    {
        // given
        referenceCheck.skip();

        // when
        IllegalStateException exception = assertThrows( IllegalStateException.class, () -> referenceCheck.checkReference( new PropertyRecord( 0 ), null ) );
        assertEquals( "Reference has already been checked.", exception.getMessage() );
    }

    @Test
    void shouldNotAllowCheckDiffReferenceAfterSkip()
    {
        // given
        referenceCheck.skip();

        // when
        IllegalStateException exception =
                assertThrows( IllegalStateException.class, () -> referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null ) );
        assertEquals( "Reference has already been checked.", exception.getMessage() );
    }

    @Test
    void shouldNotAllowCheckReferenceAfterCheckReference()
    {
        // given
        referenceCheck.checkReference( new PropertyRecord( 0 ), null );

        // when
        IllegalStateException exception = assertThrows( IllegalStateException.class, () -> referenceCheck.checkReference( new PropertyRecord( 0 ), null ) );
        assertEquals( "Reference has already been checked.", exception.getMessage() );
    }

    @Test
    void shouldNotAllowCheckDiffReferenceAfterCheckReference()
    {
        // given
        referenceCheck.checkReference( new PropertyRecord( 0 ), null );

        // when
        IllegalStateException exception =
                assertThrows( IllegalStateException.class, () -> referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null ) );
        assertEquals( "Reference has already been checked.", exception.getMessage() );
    }

    @Test
    void shouldNotAllowCheckReferenceAfterCheckDiffReference()
    {
        // given
        referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null );

        // when
        IllegalStateException exception =
                assertThrows( IllegalStateException.class, () -> referenceCheck.checkReference( new PropertyRecord( 0 ), null ) );
        assertEquals( "Reference has already been checked.", exception.getMessage() );
    }

    @Test
    void shouldNotAllowCheckDiffReferenceAfterCheckDiffReference()
    {
        // given
        referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null );

        // when
        IllegalStateException exception =
                assertThrows( IllegalStateException.class, () -> referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null ) );
        assertEquals( "Reference has already been checked.", exception.getMessage() );
    }
}
