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
package org.neo4j.helpers.progress;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.helpers.ProcessFailureException;
import org.neo4j.test.SuppressOutput;

import static java.lang.System.lineSeparator;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class ProgressMonitorTest
{

    @Test
    public void shouldReportProgressInTheSpecifiedIntervals() throws Exception
    {
        // given
        Indicator indicator = indicatorMock();
        ProgressListener progressListener = factory.mock( indicator, 10 ).singlePart( testName.getMethodName(), 16 );

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
    public void shouldAggregateProgressFromMultipleProcesses() throws Exception
    {
        // given
        Indicator indicator = indicatorMock();
        ProgressMonitorFactory.MultiPartBuilder builder = factory.mock( indicator, 10 ).multipleParts( testName.getMethodName() );
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
    public void shouldNotAllowAddingPartsAfterCompletingMultiPartBuilder() throws Exception
    {
        // given
        ProgressMonitorFactory.MultiPartBuilder builder = factory.mock( indicatorMock(), 10 )
                                                   .multipleParts( testName.getMethodName() );
        builder.progressForPart( "first", 10 );
        builder.build();

        // when
        try
        {
            builder.progressForPart( "other", 10 );
            fail( "should have thrown exception" );
        }
        // then
        catch ( IllegalStateException expected )
        {
            assertEquals( "Builder has been completed.", expected.getMessage() );
        }
    }

    @Test
    public void shouldNotAllowAddingMultiplePartsWithSameIdentifier() throws Exception
    {
        // given
        ProgressMonitorFactory.MultiPartBuilder builder = Mockito.mock( ProgressMonitorFactory.class )
                                                   .multipleParts( testName.getMethodName() );
        builder.progressForPart( "first", 10 );

        // when
        try
        {
            builder.progressForPart( "first", 10 );
            fail( "should have thrown exception" );
        }
        // then
        catch ( IllegalArgumentException expected )
        {
            assertEquals( "Part 'first' has already been defined.", expected.getMessage() );
        }
    }

    @Test
    public void shouldStartProcessAutomaticallyIfNotDoneBefore() throws Exception
    {
        // given
        Indicator indicator = indicatorMock();
        ProgressListener progressListener = factory.mock( indicator, 10 ).singlePart( testName.getMethodName(), 16 );

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
    public void shouldStartMultiPartProcessAutomaticallyIfNotDoneBefore() throws Exception
    {
        // given
        Indicator indicator = indicatorMock();
        ProgressMonitorFactory.MultiPartBuilder builder = factory.mock( indicator, 10 ).multipleParts( testName.getMethodName() );
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
    public void shouldCompleteMultiPartProgressWithNoPartsImmediately() throws Exception
    {
        // given
        Indicator indicator = indicatorMock();
        ProgressMonitorFactory.MultiPartBuilder builder = factory.mock( indicator, 10 ).multipleParts( testName.getMethodName() );

        // when
        builder.build();

        // then
        InOrder order = inOrder( indicator );
        order.verify( indicator ).startProcess( 0 );
        order.verify( indicator ).progress( 0, 10 );
        order.verify( indicator ).completeProcess();
        order.verifyNoMoreInteractions();
    }

    private static Indicator indicatorMock()
    {
        Indicator indicator = mock( Indicator.class, Mockito.CALLS_REAL_METHODS );
        doNothing().when( indicator ).progress( anyInt(), anyInt() );
        return indicator;
    }

    private static final String EXPECTED_TEXTUAL_OUTPUT;

    static
    {
        StringWriter expectedTextualOutput = new StringWriter();
        for ( int i = 0; i < 10; )
        {
            for ( int j = 0; j < 20; j++ )
            {
                expectedTextualOutput.write( '.' );
            }
            expectedTextualOutput.write( String.format( " %3d%%%n", (++i) * 10 ) );
        }
        EXPECTED_TEXTUAL_OUTPUT = expectedTextualOutput.toString();
    }

    @Test
    public void shouldPrintADotEveryHalfPercentAndFullPercentageEveryTenPercentWithTextualIndicator() throws Exception
    {
        // given
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ProgressListener progressListener = ProgressMonitorFactory.textual( stream ).singlePart( testName.getMethodName(), 1000 );

        // when
        for ( int i = 0; i < 1000; i++ )
        {
            progressListener.add( 1 );
        }

        // then
        assertEquals( testName.getMethodName() + lineSeparator() + EXPECTED_TEXTUAL_OUTPUT,
                      stream.toString( Charset.defaultCharset().name() ) );
    }

    @Test
    public void shouldPrintADotEveryHalfPercentAndFullPercentageEveryTenPercentEvenWhenStepResolutionIsLower()
            throws Exception
    {
        // given
        StringWriter writer = new StringWriter();
        ProgressListener progressListener = ProgressMonitorFactory.textual( writer ).singlePart( testName.getMethodName(), 50 );

        // when
        for ( int i = 0; i < 50; i++ )
        {
            progressListener.add( 1 );
        }

        // then
        assertEquals( testName.getMethodName() + lineSeparator() + EXPECTED_TEXTUAL_OUTPUT,
                      writer.toString() );
    }

    @Test
    public void shouldPassThroughAllInvocationsOnDecorator() throws Exception
    {
        // given
        Indicator decorated = mock( Indicator.class );
        Indicator decorator = new Indicator.Decorator( decorated )
        {
        };

        // when
        decorator.startProcess( 4 );
        // then
        verify( decorated ).startProcess( 4 );

        // when
        decorator.startPart( "part1", 2 );
        // then
        verify( decorated ).startPart( "part1", 2 );

        // when
        decorator.progress( 0, 1 );
        // then
        verify( decorated ).progress( 0, 1 );

        // when
        decorator.startPart( "part2", 2 );
        // then
        verify( decorated ).startPart( "part2", 2 );

        // when
        decorator.progress( 1, 2 );
        // then
        verify( decorated ).progress( 1, 2 );

        // when
        decorator.completePart( "part1" );
        // then
        verify( decorated ).completePart( "part1" );

        // when
        decorator.progress( 2, 3 );
        // then
        verify( decorated ).progress( 2, 3 );

        // when
        decorator.completePart( "part2" );
        // then
        verify( decorated ).completePart( "part2" );

        // when
        decorator.progress( 3, 4 );
        // then
        verify( decorated ).progress( 3, 4 );

        // when
        decorator.completeProcess();
        // then
        verify( decorated ).completeProcess();
    }

    @Test
    public void shouldBeAbleToAwaitCompletionOfMultiPartProgress() throws Exception
    {
        // given
        ProgressMonitorFactory.MultiPartBuilder builder = ProgressMonitorFactory.NONE.multipleParts( testName.getMethodName() );
        ProgressListener part1 = builder.progressForPart( "part1", 1 );
        ProgressListener part2 = builder.progressForPart( "part2", 1 );
        final Completion completion = builder.build();

        // when
        final CountDownLatch begin = new CountDownLatch( 1 ), end = new CountDownLatch( 1 );
        new Thread()
        {
            @Override
            public void run()
            {
                begin.countDown();
                try
                {
                    completion.await( 1, SECONDS );
                }
                catch ( Exception e )
                {
                    return; // do not count down the end latch
                }
                end.countDown();
            }
        }.start();
        Runnable callback = mock( Runnable.class );
        completion.notify( callback );
        assertTrue( begin.await( 1, SECONDS ) );

        // then
        verifyZeroInteractions( callback );

        // when
        try
        {
            completion.await( 1, TimeUnit.MILLISECONDS );
            fail( "should have thrown exception" );
        }
        // then
        catch ( TimeoutException expected )
        {
            assertEquals( "Process did not complete within 1 MILLISECONDS.", expected.getMessage() );
        }

        // when
        part1.done();
        // then
        verifyZeroInteractions( callback );

        // when
        part2.done();
        // then
        verify( callback ).run();
        completion.await( 0, TimeUnit.NANOSECONDS ); // should not have to wait
        assertTrue( end.await( 1, SECONDS ) ); // should have been completed

        // when
        callback = mock( Runnable.class );
        completion.notify( callback );
        verify( callback ).run();
    }

    @Test
    public void shouldReturnToCompletionWaiterWhenFirstJobFails() throws Exception
    {
        // given
        ProgressMonitorFactory.MultiPartBuilder builder = ProgressMonitorFactory.NONE.multipleParts( testName.getMethodName() );
        ProgressListener part1 = builder.progressForPart( "part1", 1 );
        ProgressListener part2 = builder.progressForPart( "part2", 1 );
        final Completion completion = builder.build();

        // when
        part1.started();
        part2.started();
        part2.failed( new RuntimeException( "failure in one of the jobs" ) );

        // neither job completes

        try
        {
            completion.await( 1, TimeUnit.MILLISECONDS );
            fail( "should have thrown exception" );
        }
        // then
        catch ( ProcessFailureException expected )
        {
            assertEquals( "failure in one of the jobs", expected.getCause().getMessage() );
        }
    }

    @Test
    public void shouldNotAllowNullCompletionCallbacks() throws Exception
    {
        ProgressMonitorFactory.MultiPartBuilder builder = ProgressMonitorFactory.NONE.multipleParts( testName.getMethodName() );
        Completion completion = builder.build();

        // when
        try
        {
            completion.notify( null );
            fail( "should have thrown exception" );
        }
        // then
        catch ( IllegalArgumentException expected )
        {
            assertEquals( "callback may not be null", expected.getMessage() );
        }
    }

    @Test
    public void shouldInvokeAllCallbacksEvenWhenOneThrowsException() throws Exception
    {
        // given
        ProgressMonitorFactory.MultiPartBuilder builder = ProgressMonitorFactory.NONE.multipleParts( testName.getMethodName() );
        ProgressListener progressListener = builder.progressForPart( "only part", 1 );
        Completion completion = builder.build();
        Runnable callback = mock( Runnable.class );
        doThrow( new RuntimeException( "on purpose" ) ).doNothing().when( callback ).run();
        completion.notify( callback );
        completion.notify( callback );

        // when
        progressListener.done();

        // then
        verify( callback, times( 2 ) ).run();
    }

    @Test
    public void shouldAllowStartingAPartBeforeCompletionOfMultiPartBuilder() throws Exception
    {
        // given
        Indicator indicator = mock( Indicator.class );
        ProgressMonitorFactory.MultiPartBuilder builder = factory.mock( indicator, 10 ).multipleParts( testName.getMethodName() );
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

    @Test
    public void shouldAllowOpenEndedProgressListeners() throws Exception
    {
        // given
        Indicator.OpenEnded indicator = mock( Indicator.OpenEnded.class );
        ProgressListener progress = factory.mock( indicator ).openEnded( testName.getMethodName(), 10 );

        // when
        for ( int i = 0; i < 20; i++ )
        {
            progress.add( 5 );
        }
        progress.done();

        // then
        verify( indicator, atLeast( 1 ) ).reportResolution();
        InOrder order = inOrder( indicator );
        order.verify( indicator ).startProcess( 0 );
        for ( int i = 0; i < 10; i++ )
        {
            order.verify( indicator ).progress( i, i + 1 );
        }
        order.verify( indicator ).completeProcess();
        verifyNoMoreInteractions( indicator );
    }

    @Test
    public void shouldReportOpenEndedProgressInANiceWay() throws Exception
    {
        // given
        StringWriter buffer = new StringWriter();
        ProgressListener progress = ProgressMonitorFactory.textual( buffer ).openEnded( testName.getMethodName(), 10 );

        // when
        for ( int i = 0; i < 25; i++ )
        {
            progress.add( 50 );
        }
        progress.done();

        // then
        assertEquals( String.format(
                testName.getMethodName() + "%n" +
                "....................     200%n" +
                "....................     400%n" +
                "....................     600%n" +
                "....................     800%n" +
                "....................    1000%n" +
                "....................    1200%n" +
                ".....                   done%n" ), buffer.toString() );
    }

    @Rule
    public final TestName testName = new TestName();
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public final SingleIndicator factory = new SingleIndicator();

    private static class SingleIndicator implements TestRule
    {
        ProgressMonitorFactory mock( Indicator indicatorMock, int indicatorSteps )
        {
            when( indicatorMock.reportResolution() ).thenReturn( indicatorSteps );
            ProgressMonitorFactory factory = Mockito.mock( ProgressMonitorFactory.class );
            when( factory.newIndicator( any( String.class ) ) ).thenReturn( indicatorMock );
            factoryMocks.put( factory, false );
            return factory;
        }

        ProgressMonitorFactory mock( final Indicator.OpenEnded indicatorMock )
        {
            ProgressMonitorFactory factory = Mockito.mock( ProgressMonitorFactory.class );
            when( factory.newOpenEndedIndicator( any( String.class ), anyInt() ) ).thenAnswer( new Answer<Indicator>()
            {
                @Override
                public Indicator answer( InvocationOnMock invocation ) throws Throwable
                {
                    when( indicatorMock.reportResolution() ).thenReturn( (Integer) invocation.getArguments()[1] );
                    return indicatorMock;
                }
            } );
            factoryMocks.put( factory, true );
            return factory;
        }

        private final Map<ProgressMonitorFactory,Boolean> factoryMocks = new HashMap<>();

        @Override
        public Statement apply( final Statement base, Description description )
        {
            return new Statement()
            {
                @Override
                public void evaluate() throws Throwable
                {
                    base.evaluate();
                    for ( Map.Entry<ProgressMonitorFactory,Boolean> factoryMock : factoryMocks.entrySet() )
                    {
                        if ( factoryMock.getValue() )
                        {
                            verify( factoryMock.getKey(), times( 1 ) ).newOpenEndedIndicator( any( String.class ),
                                                                                              anyInt() );
                        }
                        else
                        {
                            verify( factoryMock.getKey(), times( 1 ) ).newIndicator( any( String.class ) );
                        }
                    }
                }
            };
        }
    }
}
