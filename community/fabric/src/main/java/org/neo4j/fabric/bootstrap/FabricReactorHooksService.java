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
package org.neo4j.fabric.bootstrap;

import reactor.core.publisher.Hooks;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.neo4j.fabric.transaction.ErrorReporter;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

class FabricReactorHooksService extends LifecycleAdapter
{
    // DESIGN NOTES:
    // Only one Neo4j server should run in a single JVM in most deployments
    // and only one error consumer will be registered in such cases.
    // However, if more than one Neo4j server run in a single JVM an error consumer
    // will be registered for each of them. This means that if an error in a Reactor stream is dropped
    // in any of them, the error will be reported in a log of every server.
    // This cannot be prevented as the error cannot be linked to an instance where it occurred.
    // This is not such a big problem as this type of errors signify Fabric bugs and, ideally, should not happen.
    // And if they occur reporting them in every Neo4j server on a JVM is not such a problem.
    // The hook is never unregistered from Reactor, because the API does not allow unregistering a concrete hook,
    // but provides only an operation that removes all of them. If that operation was used, it would mean removing
    // hooks potentially added by the users outside Neo4j code base for instance in extensions or even applications
    // if the server is used in embedded mode.

    private static final Set<Consumer<? super Throwable>> ERROR_CONSUMERS = ConcurrentHashMap.newKeySet();

    static
    {
        Hooks.onErrorDropped( e -> ERROR_CONSUMERS.forEach( consumer -> consumer.accept( e ) ) );
    }

    private final Consumer<? super Throwable> errorConsumer;

    FabricReactorHooksService( ErrorReporter errorReporter )
    {
        errorConsumer = throwable -> errorReporter.report( "Unhandled error", throwable, Status.General.UnknownError );
        ERROR_CONSUMERS.add( errorConsumer );
    }

    @Override
    public void stop()
    {
        ERROR_CONSUMERS.remove( errorConsumer );
    }
}
