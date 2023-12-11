/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb.facade;

import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.internal.helpers.collection.Iterables.empty;

import java.util.Iterator;
import java.util.List;
import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.service.Services;

public class GraphDatabaseDependencies implements ExternalDependencies {
    public static GraphDatabaseDependencies newDependencies(ExternalDependencies deps) {
        return new GraphDatabaseDependencies(
                deps.monitors(),
                deps.userLogProvider(),
                deps.dependencies(),
                deps.extensions(),
                deps.databaseEventListeners());
    }

    public static GraphDatabaseDependencies newDependencies() {
        Iterable<ExtensionFactory<?>> extensions =
                getExtensions(Services.loadAll(ExtensionFactory.class).iterator());
        return new GraphDatabaseDependencies(null, null, null, extensions, empty());
    }

    private Monitors monitors;
    private InternalLogProvider userLogProvider;
    private InternalLogProvider debugLogProvider;
    private DependencyResolver dependencies;
    private List<ExtensionFactory<?>> extensions;
    private List<DatabaseEventListener> databaseEventListeners;

    private GraphDatabaseDependencies(
            Monitors monitors,
            InternalLogProvider userLogProvider,
            DependencyResolver dependencies,
            Iterable<ExtensionFactory<?>> extensions,
            Iterable<DatabaseEventListener> eventListeners) {
        this.monitors = monitors;
        this.userLogProvider = userLogProvider;
        this.dependencies = dependencies;
        this.extensions = asList(extensions);
        this.databaseEventListeners = asList(eventListeners);
    }

    // Builder DSL
    public GraphDatabaseDependencies monitors(Monitors monitors) {
        this.monitors = monitors;
        return this;
    }

    public GraphDatabaseDependencies userLogProvider(InternalLogProvider userLogProvider) {
        this.userLogProvider = userLogProvider;
        return this;
    }

    public GraphDatabaseDependencies debugLogProvider(InternalLogProvider debugLogProvider) {
        this.debugLogProvider = debugLogProvider;
        return this;
    }

    public GraphDatabaseDependencies dependencies(DependencyResolver dependencies) {
        this.dependencies = dependencies;
        return this;
    }

    public GraphDatabaseDependencies databaseEventListeners(Iterable<DatabaseEventListener> eventListeners) {
        this.databaseEventListeners = asList(eventListeners);
        return this;
    }

    public GraphDatabaseDependencies extensions(Iterable<ExtensionFactory<?>> extensions) {
        this.extensions = asList(extensions);
        return this;
    }

    // Dependencies implementation
    @Override
    public Monitors monitors() {
        return monitors;
    }

    @Override
    public InternalLogProvider userLogProvider() {
        return userLogProvider;
    }

    public InternalLogProvider debugLogProvider() {
        return debugLogProvider;
    }

    @Override
    public Iterable<ExtensionFactory<?>> extensions() {
        return extensions;
    }

    @Override
    public Iterable<DatabaseEventListener> databaseEventListeners() {
        return databaseEventListeners;
    }

    @Override
    public DependencyResolver dependencies() {
        return dependencies;
    }

    // This method is needed to convert the non generic ExtensionFactory type returned from Service.load
    // to ExtensionFactory<?> generic types
    private static Iterable<ExtensionFactory<?>> getExtensions(Iterator<ExtensionFactory> parent) {
        return Iterators.asList(new Iterator<>() {
            @Override
            public boolean hasNext() {
                return parent.hasNext();
            }

            @Override
            public ExtensionFactory<?> next() {
                return parent.next();
            }
        });
    }
}
