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
package org.neo4j.kernel.monitoring.tracing;

import org.neo4j.helpers.Service;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.logging.Log;

/**
 * <h1>Tracers</h1>
 * <p>
 *     While monitoring is a dynamic piece of infrastructure, where monitors can be wired up and torn down on the fly,
 *     the tracing infrastructure is static and hard-coded into the database on startup.
 * </p>
 * <p>
 *     Tracing is always on, though the default implementation is very light weight, incurring almost no overhead.
 * </p>
 * <h2>The Tracers class</h2>
 * <p>
 *     This is the central hub in the tracing infrastructure.
 * </p>
 * <p>
 *     This class is responsible for choosing what tracing implementation to use, and for creating the relevant tracer
 *     components to distribute throughout the database instance.
 * </p>
 * <p>
 *     The tracing implementation is determined by the {@code dbms.tracer} setting. Two built-in implementations
 *     exist: {@code default} and {@code null}. Alternative implementations can be loaded from the
 *     classpath by referencing their {@link org.neo4j.kernel.monitoring.tracing.TracerFactory} in a
 *     {@code META-INF/services/org.neo4j.kernel.monitoring.tracing.TracerFactory}, and setting
 *     {@code dbms.tracer} to the appropriate value.
 * </p>
 * <h2>Designing and implementing tracers</h2>
 * <p>
 *     There are two parts to tracers: the tracer implementation, which starts with the TracerFactory, and the
 *     subsystems that expose themselves to tracing.
 * </p>
 * <p>
 *     The traced subsystems are responsible for defining their own tracer and trace event interfaces, and they are
 *     responsible for getting hold of a tracer implementation through the Tracers class, and for producing trace
 *     events and feeding them with data.
 * </p>
 * <p>
 *     Traced subsystems define a hierarchy of events: large coarse grain events can contain smaller and more
 *     detailed events. Sibling events (that follow one after another in time) are typically spawned from the same
 *     parent event. The tracers and trace events are all defined as interfaces, and each interface should have a
 *     {@code NULL} field that references an implementation that does nothing, other than return other {@code NULL}
 *     implementations of any child event interfaces. The existing trace interfaces for transactions and the page
 *     cache, are good examples of this.
 * </p>
 * <p>
 *     The tracer implementations are responsible for implementing all the tracer and trace event interfaces in a way
 *     that is both fast, and robust. Robustness is important because tracer implementations are not allowed to throw
 *     exceptions, and they are not allowed to return {@code null} where a trace event is expected. Implementations
 *     may add implementation specific data to the events, if they want to report implementation specific data.
 *     They are also allowed to produce the {@code NULL} implementations that are associated with the various tracer
 *     and trace event interfaces. If, for instance, the implementation is not interested in the data that would be
 *     collected from a given trace event, then it can choose to use the {@code NULL} implementation. It could also
 *     be that something went wrong when building an event instance of the desired type, and since it cannot return
 *     {@code null} or throw exceptions, it is forced to return the {@code NULL} implementation.
 * </p>
 * <p>
 *     Tracer implementations should prefer to always return the same trace event implementation type for a given
 *     trace event type. Using more than one implementation type impairs JIT optimisation, as it causes the callsites
 *     in the traced code to no longer be monomorphic. Implementations should be built with performance in mind, as
 *     the code being traced is often quite important for the performance of the database.
 * </p>
 * <p>
 *     The {@code default} and {@code null} implementation are always available, and 3rd party implementations can
 *     piggy-back on them and extend them. At least one 3rd party implementation is known at this point; the
 *     <a href="https://github.com/neo4j-contrib/neo4j-jfr">neo4j-jfr implementation</a>. It is recommended that
 *     those change the tracer or trace event interfaces, or add tracing to more subsystems, also make sure to keep
 *     the neo4j-jfr code base up to date.
 * </p>
 */
public class Tracers
{
    public final PageCacheTracer pageCacheTracer;
    public final TransactionTracer transactionTracer;
    public final CheckPointTracer checkPointTracer;

    /**
     * Create a Tracers subsystem with the desired implementation, if it can be found and created.
     *
     * Otherwise the default implementation is used, and a warning is logged to the given StringLogger.
     * @param desiredImplementationName The name of the desired {@link org.neo4j.kernel.monitoring.tracing
     * .TracerFactory} implementation, as given by its {@link TracerFactory#getImplementationName()} method.
     * @param msgLog A {@link Log} for logging when the desired implementation cannot be created.
     */
    public Tracers( String desiredImplementationName, Log msgLog )
    {
        if ( "null".equalsIgnoreCase( desiredImplementationName ) )
        {
            pageCacheTracer = PageCacheTracer.NULL;
            transactionTracer = TransactionTracer.NULL;
            checkPointTracer = CheckPointTracer.NULL;
        }
        else
        {
            TracerFactory foundFactory = new DefaultTracerFactory();
            boolean found = desiredImplementationName == null;
            for ( TracerFactory factory : Service.load( TracerFactory.class ) )
            {
                try
                {
                    if ( factory.getImplementationName().equalsIgnoreCase( desiredImplementationName ) )
                    {
                        foundFactory = factory;
                        found = true;
                        break;
                    }
                }
                catch ( Exception e )
                {
                    msgLog.warn( "Failed to instantiate desired tracer implementations '" +
                                 desiredImplementationName + "'", e );
                }
            }

            if ( !found )
            {
                msgLog.warn( "Using default tracer implementations instead of '%s'", desiredImplementationName );
            }

            pageCacheTracer = foundFactory.createPageCacheTracer();
            transactionTracer = foundFactory.createTransactionTracer();
            checkPointTracer = foundFactory.createCheckPointTracer();
        }
    }
}
