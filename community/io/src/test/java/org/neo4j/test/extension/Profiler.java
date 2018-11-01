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
package org.neo4j.test.extension;

import org.junit.jupiter.api.extension.Extension;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.function.Consumer;

import org.neo4j.graphdb.Resource;

/**
 * A sampling profiler extension for JUnit 5. This extension profiles a given set of threads that run in a unit test, and if the test fails, prints a profile
 * of where the time was spent. This is particularly useful for tests that has a tendency to fail with a timeout, and this extension can be used to diagnose
 * such flaky tests.
 * <p>
 * The profile output is printed to {@link System#err} by default, but this can be changed, and doing so is recommended in order to capture output from tests
 * that are running in a Surefire forked JVM.
 * <p>
 * Here is an example of how to use it:
 *
 * <pre><code>
 *     public class MyTest
 *     {
 *         {@literal @}RegisterExtension
 *         public Profiler profiler = Profiler.profiler();
 *
 *         {@literal @}Test
 *         void testSomeStuff()
 *         {
 *             profiler.profile();
 *             // ... do some stuff in this thread.
 *         }
 *     }
 * </code></pre>
 * <p>
 * It is also possible to configure the profiler to send the output to a {@link File}, or a {@link PrintStream}, of your choice. And the application of
 * configurations can be delayed until just before the first test runs, using the {@link ProfilerConfig#delayedConfig(Consumer)} method. The profiling of a
 * thread can also be delayed by some fixed amount of time, to heuristically skip over some initial classloading, object allocation, warmup, etc.
 * The {@link #profile()} methods also returns a {@link Resource} that, when closed, will stop the profiling of the given thread. Using this with a
 * try-with-resources further helps to filter out uninteresting parts of the test.
 * <p>
 * Here is a more advanced example that showcases these features:
 *
 * <pre><code>
 *     {@literal @}ExtendWith( TestDirectoryExtension.class )
 *     public class MyTest
 *     {
 *         {@literal @}Inject
 *         public TestDirectory directory;
 *
 *         {@literal @}RegisterExtension
 *         public Profiler profiler = Profiler.config()
 *                 .delayedConfig( cfg -> cfg.outputTo( directory.createFile( "profiler-output.txt" ) ) )
 *                 .closeOutputWhenDone( true )
 *                 .enableOutputOnSuccess( true ).profiler();
 *
 *         {@literal @}Test
 *         void testSomeStuff()
 *         {
 *             Thread someThreadA = new Thread( () -> doSomeWorkForTheTest() );
 *             someThreadA.start();
 *             Thread someThreadB = new Thread( () -> doSomeOtherWork() );
 *             someThreadB.start();
 *             profiler.profile( someThreadA, TimeUnit.MILLISECONDS.toNanos( 100 ) );
 *             profiler.profile( someThreadB, TimeUnit.MILLISECONDS.toNanos( 100 ) );
 *
 *             try ( Resource profiledRegion = profiler.profile() )
 *             {
 *                 // ... the test of the test.
 *             }
 *
 *             someThreadA.join();
 *             someThreadB.join();
 *         }
 *     }
 * </code></pre>
 */
public interface Profiler extends Extension
{
    /**
     * A profiler implementation that does not do anything, and returns a {@code null} resource.
     * <p>
     * Note that try-with-resources clauses cope well with {@code null} resources, so you normally do not need to handle that in any special way.
     */
    Profiler NULL = ( thread, delay ) -> null;

    /**
     * @return a {@link Profiler} extension with default configuration.
     */
    static Profiler profiler()
    {
        return new ProfilerExtension();
    }

    /**
     * @return A {@link ProfilerConfig} builder, which can construct a {@link Profiler} with a particular configuration.
     */
    static ProfilerConfig config()
    {
        return new ProfilerExtension().new Configurator();
    }

    /**
     * Start profiling the current thread.
     * <p>
     * The profiled thread will have its stack sampled, until either the test ends, or the returned resource is closed.
     *
     * @return A resource that, when closed, will stop the profiling of the current thread.
     */
    default Resource profile()
    {
        return profile( Thread.currentThread() );
    }

    /**
     * Start profiling the given thread.
     * <p>
     * The profiled thread will have its stack sampled, until either the test ends, or the returned resource is closed.
     *
     * @param threadToProfile The thread to profile.
     * @return A resource that, when closed, will stop profiling the given thread.
     */
    default Resource profile( Thread threadToProfile )
    {
        return profile( threadToProfile, 0 );
    }

    /**
     * Start profiling the given thread after the given delay in nanoseconds.
     * <p>
     * The profiled thread will have its stack sampled, until either the test ends, or the returned resource is closed.
     *
     * @param threadToProfile The thread to profile.
     * @param initialDelayNanos The profiling will not start until after this delay in nanoseconds has transpired.
     * @return A resource that, when closed, will stop the profiling of the given thread.
     */
    Resource profile( Thread threadToProfile, long initialDelayNanos );

    /**
     * A builder that configures, and ultimately returns a {@link Profiler} from the {@link ProfilerConfig#profiler()} method.
     */
    interface ProfilerConfig
    {
        /**
         * Set to {@code true} if the profiler should produce output even if the tests succeed.
         * <p>
         * Default is {@code false}.
         */
        ProfilerConfig enableOutputOnSuccess( boolean enabled );

        /**
         * Send the profiler output to the given {@link PrintStream}, instead of {@link System#err} which is the default.
         */
        ProfilerConfig outputTo( PrintStream out );

        /**
         * Send the profiler output to the given {@link File}, instead of {@link System#err} which is the default.
         */
        default ProfilerConfig outputTo( File file )
        {
            try
            {
                return outputTo( new PrintStream( file ) );
            }
            catch ( FileNotFoundException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        /**
         * Set to {@code true} if the output stream should be closed when the test is done.
         * <p>
         * This is {@code false} by default, because the output stream is {@link System#err} by default, which should not be closed.
         */
        ProfilerConfig closeOutputWhenDone( boolean closeOutputWhenDone );

        /**
         * Change the sampling interval to the given interval in nanoseconds.
         * <p>
         * The sampling interval is 10 milliseconds by default.
         */
        ProfilerConfig sampleIntervalNanos( long sampleIntervalNanos );

        /**
         * Apply the given just before the next test is executed. Multiple configuration changes can be piled up by calling this method multiple times.
         * The collected delayed configuration changes are only applied immediately before running the <em>next</em> test, and then the list of delayed changes
         * is cleared. A delayed configuration change is allowed to add other delayed configuration changes, but they will not be applied until the test
         * <em>after</em> the next. This way, a delayed configuration change can be either applied once before the first test is executed, or it can be applied
         * before each test is executed if it also delayes itself again every time it is applied.
         */
        ProfilerConfig delayedConfig( Consumer<ProfilerConfig> delayedConfigChange );

        /**
         * @return the configured profiler.
         */
        Profiler profiler();
    }
}
