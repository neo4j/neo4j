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
package org.neo4j.server.plugins;

import java.util.Collection;

import org.apache.commons.configuration.Configuration;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 *  Interface to be implemented and exposed via the Java ServiceLocator mechanism that allows
 *  plugins to provide their own initialization.<br>
 *  The implementations of this interface have to be listed in a file
 *  META-INF/services/org.neo4j.server.plugins.PluginLifecycle
 *  that contains the fully qualified class names of the individual plugin. This file
 *  has to be supplied with the plugin jar to the Neo4j server.<br>
 *  The plugin might return a collection of {@link Injectable}s that can later be used with
 *  {@literal @Context} injections.
 */
public interface PluginLifecycle
{
    /**
     * Called at initialization time, before the plugin ressources are acutally loaded.
     * @param graphDatabaseService of the Neo4j service, use it to integrate it with custom configuration mechanisms
     * @param config server configuration
     * @return A list of {@link Injectable}s that will be available to resource dependency injection later
     */
    Collection<Injectable<?>> start( GraphDatabaseService graphDatabaseService, Configuration config );

    /**
     * called to shutdown individual external resources or configurations
     */
    void stop();
}
