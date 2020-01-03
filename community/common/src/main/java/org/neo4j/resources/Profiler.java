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
package org.neo4j.resources;

import java.io.PrintStream;

/**
 * An API for profiling threads, to see where they spend their time.
 * <p>
 * The profiler is created with the {@link #profiler()} method, and is ready to use. Threads are profiled individually, and profiling is started with the
 * {@link #profile()} family of methods. Profiling can be selectively stopped by closing the returned {@link ProfiledInterval} instance. When you are done
 * collecting profiling data, call {@link #finish()}. The {@link #finish()} mehtod must be called before the profiling data can be printed, and calling
 * {@link #finish()} will cause all on-going profiling to stop. Once the profiling has finished, the profile data can be printed with the
 * {@link #printProfile(PrintStream, String)} method.
 * <p>
 * If you want to use the profiler again, you must call {@link #reset()} before you can start profiling again.
 */
public interface Profiler
{
    /**
     * @return a {@link Profiler} that does not actually do any profiling.
     */
    static Profiler nullProfiler()
    {
        return new NullProfiler();
    }

    /**
     * @return a stack-sampling {@link Profiler}.
     */
    static Profiler profiler()
    {
        return new SamplingProfiler();
    }

    /**
     * Reset the state of the profiler, and clear out all collected profile data.
     * <p>
     * Call this if you want to use the profiler again, after having collected profiling data, {@link #finish() finished} profiling,
     * and {@link #printProfile(PrintStream, String) printed} your profiling data.
     */
    void reset();

    /**
     * Set the sampling interval, as the desired nanoseconds between samples.
     * @param nanos The desired nanoseconds between profiling samples.
     */
    void setSampleIntervalNanos( long nanos );

    /**
     * Start profiling the current thread.
     * <p>
     * The profiled thread will have its stack sampled, until either the test ends, or the returned resource is closed.
     *
     * @return A resource that, when closed, will stop the profiling of the current thread.
     */
    default ProfiledInterval profile()
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
    default ProfiledInterval profile( Thread threadToProfile )
    {
        return profile( threadToProfile, 0 );
    }

    /**
     * Finish the profiling, which includes stopping and waiting for the termination of all on-going profiles, and then prepare the collected profiling data
     * for printing. This method must be called before {@link #printProfile(PrintStream, String)} can be called.
     *
     * @throws InterruptedException If the thread was interrupted while waiting for all on-going profiling activities to stop.
     */
    void finish() throws InterruptedException;

    /**
     * Write out a textual representation of the collected profiling data, to the given {@link PrintStream}.
     * <p>
     * The report will start with the given profile title, and will have a "sorted sample tree" printed for each of the profiled threads.
     *
     * @param out the print stream where the output will be written to.
     * @param profileTitle the title of the profile report.
     */
    void printProfile( PrintStream out, String profileTitle );

    /**
     * Start profiling the given thread after the given delay in nanoseconds.
     * <p>
     * The profiled thread will have its stack sampled, until either the test ends, or the returned resource is closed.
     *
     * @param threadToProfile The thread to profile.
     * @param initialDelayNanos The profiling will not start until after this delay in nanoseconds has transpired.
     * @return A resource that, when closed, will stop the profiling of the given thread.
     */
    ProfiledInterval profile( Thread threadToProfile, long initialDelayNanos );

    /**
     * When closed, will cause the on-going profiling of a thread to be stopped.
     */
    interface ProfiledInterval extends AutoCloseable
    {
        @Override
        void close();
    }
}
