/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.helpers.progress;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;

import org.neo4j.test.extension.SuppressOutputExtension;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@ExtendWith( SuppressOutputExtension.class )
class ProgressMonitorTest
{
    private static final String EXPECTED_TEXTUAL_OUTPUT = buildExpectedOutput();
    private Indicator indicator;
    private ProgressMonitorFactory factory;

    @BeforeEach
    void setUp()
    {
        indicator = indicatorMock();
        when( indicator.reportResolution() ).thenReturn( 10 );
        factory = mock( ProgressMonitorFactory.class );
        when( factory.newIndicator( any( String.class ) ) ).thenReturn( indicator );
    }

    @Test
    void shouldReportProgressInTheSpecifiedIntervals( TestInfo testInfo )
    {
        // given
        ProgressListener progressListener = factory.singlePart( testInfo.getDisplayName(), 16 );

        // when
        progressListener.started();
        for ( int i = 0; i < 16; i++ )
        {
            progressListener.add( 1 );
        }
        progressListener.done();

        // then
        InOrder order = inOrder( indicator );
        order.verify( indicator ).startProcess( 16 );
        for ( int i = 0; i < 10; i++ )
        {
            order.verify( indicator ).progress( i, i + 1 );
        }
        order.verify( indicator ).completeProcess();
        order.verifyNoMoreInteractions();
    }

    @Test
    void shouldAggregateProgressFromMultipleProcesses( TestInfo testInfo )
    {
        // given
        ProgressMonitorFactory.MultiPartBuilder builder = factory.multipleParts( testInfo.getDisplayName() );
        ProgressListener first = builder.progressForPart( "first", 5 );
        ProgressListener other = builder.progressForPart( "other", 5 );
        builder.build();
        InOrder order = inOrder( indicator );
        order.verify( indicator ).startProcess( 10 );
        order.verifyNoMoreInteractions();

        // when
        first.started();
        for ( int i = 0; i < 5; i++ )
        {
            first.add( 1 );
        }
        first.done();

        // then
        order.verify( indicator ).startPart( "first", 5 );
        for ( int i = 0; i < 5; i++ )
        {
            order.verify( indicator ).progress( i, i + 1 );
        }
        order.verify( indicator ).completePart( "first" );
        order.verifyNoMoreInteractions();

        // when
        other.started();
        for ( int i = 0; i < 5; i++ )
        {
            other.add( 1 );
        }
        other.done();

        // then
        order.verify( indicator ).startPart( "other", 5 );
        for ( int i = 5; i < 10; i++ )
        {
            order.verify( indicator ).progress( i, i + 1 );
        }
        order.verify( indicator ).completePart( "other" );
        order.verify( indicator ).completeProcess();
        order.verifyNoMoreInteractions();
    }

    @Test
    void shouldNotAllowAddingPartsAfterCompletingMultiPartBuilder( TestInfo testInfo )
    {
        ProgressMonitorFactory.MultiPartBuilder builder = factory.multipleParts( testInfo.getDisplayName() );
        builder.progressForPart( "first", 10 );
        builder.build();

        IllegalStateException exception = assertThrows( IllegalStateException.class, () -> builder.progressForPart( "other", 10 ) );
        assertEquals( "Builder has been completed.", exception.getMessage() );
    }

    @Test
    void shouldNotAllowAddingMultiplePartsWithSameIdentifier( TestInfo testInfo )
    {
        ProgressMonitorFactory.MultiPartBuilder builder = mock( ProgressMonitorFactory.class )
                                                   .multipleParts( testInfo.getDisplayName() );
        builder.progressForPart( "first", 10 );

        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> builder.progressForPart( "first", 10 ) );
        assertEquals( "Part 'first' has already been defined.", exception.getMessage() );
    }

    @Test
    void shouldStartProcessAutomaticallyIfNotDoneBefore( TestInfo testInfo )
    {
        // given
        ProgressListener progressListener = factory.singlePart( testInfo.getDisplayName(), 16 );

        // when
        for ( int i = 0; i < 16; i++ )
        {
            progressListener.add( 1 );
        }
        progressListener.done();

        // then
        InOrder order = inOrder( indicator );
        order.verify( indicator, times( 1 ) ).startProcess( 16 );
        for ( int i = 0; i < 10; i++ )
        {
            order.verify( indicator ).progress( i, i + 1 );
        }
        order.verify( indicator ).completeProcess();
        order.verifyNoMoreInteractions();
    }

    @Test
    void shouldStartMultiPartProcessAutomaticallyIfNotDoneBefore( TestInfo testInfo )
    {
        // given
        ProgressMonitorFactory.MultiPartBuilder builder = factory.multipleParts( testInfo.getDisplayName() );
        ProgressListener first = builder.progressForPart( "first", 5 );
        ProgressListener other = builder.progressForPart( "other", 5 );
        builder.build();
        InOrder order = inOrder( indicator );
        order.verify( indicator ).startProcess( 10 );
        order.verifyNoMoreInteractions();

        // when
        for ( int i = 0; i < 5; i++ )
        {
            first.add( 1 );
        }
        first.done();

        // then
        order.verify( indicator ).startPart( "first", 5 );
        for ( int i = 0; i < 5; i++ )
        {
            order.verify( indicator ).progress( i, i + 1 );
        }
        order.verify( indicator ).completePart( "first" );
        order.verifyNoMoreInteractions();

        // when
        for ( int i = 0; i < 5; i++ )
        {
            other.add( 1 );
        }
        other.done();

        // then
        order.verify( indicator ).startPart( "other", 5 );
        for ( int i = 5; i < 10; i++ )
        {
            order.verify( indicator ).progress( i, i + 1 );
        }
        order.verify( indicator ).completePart( "other" );
        order.verify( indicator ).completeProcess();
        order.verifyNoMoreInteractions();
    }

    @Test
    void shouldCompleteMultiPartProgressWithNoPartsImmediately( TestInfo testInfo )
    {
        // given
        ProgressMonitorFactory.MultiPartBuilder builder = factory.multipleParts( testInfo.getDisplayName() );

        // when
        builder.build();

        // then
        InOrder order = inOrder( indicator );
        order.verify( indicator ).startProcess( 0 );
        order.verify( indicator ).progress( 0, 10 );
        order.verify( indicator ).completeProcess();
        order.verifyNoMoreInteractions();
    }

    @Test
    void shouldPrintADotEveryHalfPercentAndFullPercentageEveryTenPercentWithTextualIndicator( TestInfo testInfo ) throws Exception
    {
        // given
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ProgressListener progressListener = ProgressMonitorFactory.textual( stream ).singlePart( testInfo.getDisplayName(), 1000 );

        // when
        for ( int i = 0; i < 1000; i++ )
        {
            progressListener.add( 1 );
        }

        // then
        assertEquals( testInfo.getDisplayName() + lineSeparator() + EXPECTED_TEXTUAL_OUTPUT,
                      stream.toString( Charset.defaultCharset().name() ) );
    }

    @Test
    void shouldPrintADotEveryHalfPercentAndFullPercentageEveryTenPercentEvenWhenStepResolutionIsLower( TestInfo testInfo )
    {
        // given
        StringWriter writer = new StringWriter();
        ProgressListener progressListener = ProgressMonitorFactory.textual( writer ).singlePart( testInfo.getDisplayName(), 50 );

        // when
        for ( int i = 0; i < 50; i++ )
        {
            progressListener.add( 1 );
        }

        // then
        assertEquals( testInfo.getDisplayName() + lineSeparator() + EXPECTED_TEXTUAL_OUTPUT,
                      writer.toString() );
    }

    @Test
    void shouldAllowStartingAPartBeforeCompletionOfMultiPartBuilder( TestInfo testInfo )
    {
        // given
        ProgressMonitorFactory.MultiPartBuilder builder = factory.multipleParts( testInfo.getDisplayName() );
        ProgressListener part1 = builder.progressForPart( "part1", 1 );
        ProgressListener part2 = builder.progressForPart( "part2", 1 );

        // when
        part1.add( 1 );
        builder.build();
        part2.add( 1 );
        part1.done();
        part2.done();

        // then
        InOrder order = inOrder( indicator );
        order.verify( indicator ).startPart( "part1", 1 );
        order.verify( indicator ).startProcess( 2 );
        order.verify( indicator ).startPart( "part2", 1 );
        order.verify( indicator ).completePart( "part1" );
        order.verify( indicator ).completePart( "part2" );
        order.verify( indicator ).completeProcess();
    }

    private static Indicator indicatorMock()
    {
        Indicator indicator = mock( Indicator.class, Mockito.CALLS_REAL_METHODS );
        doNothing().when( indicator ).progress( anyInt(), anyInt() );
        return indicator;
    }

    private static String buildExpectedOutput()
    {
        StringBuilder expectedTextualOutput = new StringBuilder();
        for ( int i = 0; i < 10; )
        {
            for ( int j = 0; j < 20; j++ )
            {
                expectedTextualOutput.append( '.' );
            }
            expectedTextualOutput.append( format( " %3d%%%n", (++i) * 10 ) );
        }
        return expectedTextualOutput.toString();
    }
}
