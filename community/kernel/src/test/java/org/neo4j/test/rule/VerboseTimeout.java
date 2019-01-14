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
package org.neo4j.test.rule;

import org.apache.commons.lang3.StringUtils;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestTimedOutException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.neo4j.diagnostics.utils.DumpUtils;

/**
 * Timeout rule implementation that print out stack traces of all threads
 * instead of just one suspect, as default implementation does.
 * <p>
 * In addition provide possibility to describe provided custom entities on timeout failure.
 * Object description can be customized by provided function, by default - toString() method will be used.
 * <p>
 * For example:
 * <pre> {@code
 * public VerboseTimeout timeout = VerboseTimeout.builder()
 *                                               .withTimeout( 50, TimeUnit.SECONDS )
 *                                               .describeOnFailure( locks )
 *                                               .build()};
 * </pre>
 * Another way to use this is to not provide a specific timeout and let normal means of specifying junit test timeout take its course, e.g:
 * <pre> {@code
 * public VerboseTimeout timeout = VerboseTimeout.builder().build();
 *  ...
 * @literal @Test( timeout = 20_000 )
 *  public void shouldTestSomething()}
 * </pre>
 *
 * @see Timeout
 */
public class VerboseTimeout extends Timeout
{
    private VerboseTimeoutBuilder timeoutBuilder;
    private Description currentTestDescription;

    private VerboseTimeout( VerboseTimeoutBuilder timeoutBuilder )
    {
        super( timeoutBuilder );
        this.timeoutBuilder = timeoutBuilder;
    }

    public static VerboseTimeoutBuilder builder()
    {
        return new VerboseTimeoutBuilder();
    }

    @Override
    public Statement apply( Statement base, Description description )
    {
        // Just pick up on which test we're currently running
        currentTestDescription = description;
        return super.apply( base, description );
    }

    @Override
    protected Statement createFailOnTimeoutStatement( Statement statement )
    {
        return new VerboseFailOnTimeout( statement, timeoutBuilder );
    }

    /**
     * Helper builder class of {@link VerboseTimeout} test rule.
     */
    public static class VerboseTimeoutBuilder extends Timeout.Builder
    {
        private TimeUnit timeUnit = TimeUnit.SECONDS;
        private long timeout;
        private List<FailureParameter<?>> additionalParameters = new ArrayList<>();

        private static Function<Object,String> toStringFunction()
        {
            return value -> value == null ? StringUtils.EMPTY : value.toString();
        }

        @Override
        public VerboseTimeoutBuilder withTimeout( long timeout, TimeUnit unit )
        {
            this.timeout = timeout;
            this.timeUnit = unit;
            return this;
        }

        public <T> VerboseTimeoutBuilder describeOnFailure( T entity, Function<T,String> descriptor )
        {
            additionalParameters.add( new FailureParameter<>( entity, descriptor ) );
            return this;
        }

        public <T> VerboseTimeoutBuilder describeOnFailure( T entity )
        {
            return describeOnFailure( entity, toStringFunction() );
        }

        @Override
        public VerboseTimeout build()
        {
            return new VerboseTimeout( this );
        }

        @Override
        protected long getTimeout()
        {
            return timeout;
        }

        @Override
        protected TimeUnit getTimeUnit()
        {
            return timeUnit;
        }

        List<FailureParameter<?>> getAdditionalParameters()
        {
            return additionalParameters;
        }

        private class FailureParameter<T>
        {
            private final T entity;
            private final Function<T,String> descriptor;

            FailureParameter( T entity, Function<T,String> descriptor )
            {
                this.entity = entity;
                this.descriptor = descriptor;
            }

            String describe()
            {
                return descriptor.apply( entity );
            }
        }
    }

    /**
     * Statement that in case of timeout, unlike junit {@link org.junit.internal.runners.statements.FailOnTimeout}
     * will print thread dumps of all threads in JVM, that should help in investigation of stuck threads.
     */
    private class VerboseFailOnTimeout extends Statement
    {
        private final Statement originalStatement;
        private final TimeUnit timeUnit;
        private final long timeout;
        private final List<VerboseTimeoutBuilder.FailureParameter<?>> additionalParameters;

        VerboseFailOnTimeout( Statement statement, VerboseTimeoutBuilder builder )
        {
            originalStatement = statement;
            timeout = builder.timeout;
            timeUnit = builder.getTimeUnit();
            additionalParameters = builder.getAdditionalParameters();
        }

        @Override
        public void evaluate() throws Throwable
        {
            CallableStatement callable = new CallableStatement();
            FutureTask<Throwable> task = new FutureTask<>( callable );
            Thread thread = new Thread( task, "Time-limited test" );
            thread.setDaemon( true );
            thread.start();
            callable.awaitStarted();
            Throwable throwable = getResult( task, thread );
            if ( throwable != null )
            {
                throw throwable;
            }
        }

        private Throwable getResult( FutureTask<Throwable> task, Thread thread ) throws Throwable
        {
            try
            {
                Throwable potentialException = timeout > 0 ? task.get( timeout, timeUnit ) : task.get();
                if ( potentialException instanceof TestTimedOutException )
                {
                    // This allows this rule to be used without a specific timeout and instead use whatever
                    // timeout the test annotation specifies itself.
                    printThreadDump();
                }
                return potentialException;
            }
            catch ( ExecutionException e )
            {
                printThreadDump();
                return e.getCause();
            }
            catch ( TimeoutException e )
            {
                printThreadDump();
                return buildTimeoutException( thread );
            }
        }

        private Throwable buildTimeoutException( Thread thread )
        {
            StackTraceElement[] stackTrace = thread.getStackTrace();
            TestTimedOutException timedOutException = new TestTimedOutException( timeout, timeUnit );
            timedOutException.setStackTrace( stackTrace );
            return timedOutException;
        }

        private class CallableStatement implements Callable<Throwable>
        {
            private final CountDownLatch startLatch = new CountDownLatch( 1 );

            @Override
            public Throwable call()
            {
                try
                {
                    startLatch.countDown();
                    originalStatement.evaluate();
                }
                catch ( Throwable e )
                {
                    return e;
                }
                return null;
            }

            void awaitStarted() throws InterruptedException
            {
                startLatch.await();
            }
        }

        private void printThreadDump()
        {
            System.err.println( String.format( "=== Test %s timed out, dumping more information ===", currentTestDescription.getDisplayName() ) );
            if ( !additionalParameters.isEmpty() )
            {
                System.err.println( "=== Requested additional parameters: ===" );
                for ( VerboseTimeoutBuilder.FailureParameter<?> additionalParameter : additionalParameters )
                {
                    System.err.println( additionalParameter.describe() );
                }
            }
            System.err.println( "=== Thread dump ===" );
            System.err.println( DumpUtils.threadDump() );
        }
    }
}
