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

import org.junit.jupiter.api.Test;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.consistency.report.ConsistencyReporter.NO_MONITOR;

public class PendingReferenceCheckTest
{
    // given
    {
        RecordAccess records = mock( RecordAccess.class );
        @SuppressWarnings( "unchecked" )
        ConsistencyReporter.ReportHandler handler =
                new ConsistencyReporter.ReportHandler(
                        mock( InconsistencyReport.class ),
                        mock( ConsistencyReporter.ProxyFactory.class ),
                        RecordType.PROPERTY,
                        records, new PropertyRecord( 0 ), NO_MONITOR );
        this.referenceCheck = new PendingReferenceCheck<>( handler, mock( ComparativeRecordChecker.class ) );
    }

    private final PendingReferenceCheck<PropertyRecord> referenceCheck;

    @Test
    public void shouldAllowSkipAfterSkip()
    {
        // given
        referenceCheck.skip();
        // when
        referenceCheck.skip();
    }

    @Test
    public void shouldAllowSkipAfterCheckReference()
    {
        // given
        referenceCheck.checkReference( new PropertyRecord( 0 ), null );
        // when
        referenceCheck.skip();
    }

    @Test
    public void shouldAllowSkipAfterCheckDiffReference()
    {
        // given
        referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null );
        // when
        referenceCheck.skip();
    }

    @Test
    public void shouldNotAllowCheckReferenceAfterSkip()
    {
        // given
        referenceCheck.skip();

        // when
        try
        {
            referenceCheck.checkReference( new PropertyRecord( 0 ), null );
            fail( "expected exception" );
        }
        // then
        catch ( IllegalStateException expected )
        {
            assertEquals( "Reference has already been checked.", expected.getMessage() );
        }
    }

    @Test
    public void shouldNotAllowCheckDiffReferenceAfterSkip()
    {
        // given
        referenceCheck.skip();

        // when
        try
        {
            referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null );
            fail( "expected exception" );
        }
        // then
        catch ( IllegalStateException expected )
        {
            assertEquals( "Reference has already been checked.", expected.getMessage() );
        }
    }

    @Test
    public void shouldNotAllowCheckReferenceAfterCheckReference()
    {
        // given
        referenceCheck.checkReference( new PropertyRecord( 0 ), null );

        // when
        try
        {
            referenceCheck.checkReference( new PropertyRecord( 0 ), null );
            fail( "expected exception" );
        }
        // then
        catch ( IllegalStateException expected )
        {
            assertEquals( "Reference has already been checked.", expected.getMessage() );
        }
    }

    @Test
    public void shouldNotAllowCheckDiffReferenceAfterCheckReference()
    {
        // given
        referenceCheck.checkReference( new PropertyRecord( 0 ), null );

        // when
        try
        {
            referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null );
            fail( "expected exception" );
        }
        // then
        catch ( IllegalStateException expected )
        {
            assertEquals( "Reference has already been checked.", expected.getMessage() );
        }
    }

    @Test
    public void shouldNotAllowCheckReferenceAfterCheckDiffReference()
    {
        // given
        referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null );

        // when
        try
        {
            referenceCheck.checkReference( new PropertyRecord( 0 ), null );
            fail( "expected exception" );
        }
        // then
        catch ( IllegalStateException expected )
        {
            assertEquals( "Reference has already been checked.", expected.getMessage() );
        }
    }

    @Test
    public void shouldNotAllowCheckDiffReferenceAfterCheckDiffReference()
    {
        // given
        referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null );

        // when
        try
        {
            referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null );
            fail( "expected exception" );
        }
        // then
        catch ( IllegalStateException expected )
        {
            assertEquals( "Reference has already been checked.", expected.getMessage() );
        }
    }
}
