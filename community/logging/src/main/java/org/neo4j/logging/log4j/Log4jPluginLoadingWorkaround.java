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
package org.neo4j.logging.log4j;

import org.apache.logging.log4j.core.config.plugins.util.PluginManager;
import org.apache.logging.log4j.core.config.plugins.util.PluginRegistry;
import org.apache.logging.log4j.core.config.plugins.util.PluginType;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.util.VisibleForTesting;

class Log4jPluginLoadingWorkaround
{
    static final String PKG_SHADED_LOG4J = "org.neo4j.logging.shaded.log4j";
    static final String PKG_OUR_PLUGINS = "org.neo4j.logging.log4j";
    static final String PKG_UNSHADED_LOG4J = "org.apache.logging.log4j.core";
    private static boolean hasLoadedLog4j;

    // The point of shading Log4j is that our version shouldn't conflict with any external Log4j dependencies embedded users may have, so
    // our internal Log4j is relocated to org.neo4j.logging.shaded.log4j. The problem lies in how Log4j's handling of plugins work.
    // First it looks for plugins Log4j2Plugins.dat (plugins listing files) files in META-INF on the class path.
    // It will find both our and any external one, and it is the first one found to decides how the mapping will be.
    // This means that we interfere with any external Log4j dependency (and vice versa) so both can't work at the same time since the Log4j
    // plugins will be mapped to either the shaded version or the non-shaded version (decided by what is first on class path) and only work for
    // either us or the external version.
    // We work around this by not having any Log4jPlugins.dat file so that we don't create problems for embedded users' Log4j.
    // We fool the internal Log4j that we have already searched for Log4j2Plugins.dat files so that it won't find any external ones, and then search in
    // the packages we specify instead.
    static synchronized void doLog4jPluginLoadingWorkaround()
    {
        if ( !hasLoadedLog4j )
        {
            internalDoLog4jPluginLoadingWorkaround( PluginRegistry.getInstance(),
                    registry -> registry.getClass().getPackage().getName().contains( "shaded" ),
                    PluginManager::addPackage, Log4jPluginLoadingWorkaround::trickLog4jThatThereAreNoFiles );

            hasLoadedLog4j = true;
        }
    }

    /**
     * Should only be called from the synchronized method above, but exposed for tests.
     */
    @VisibleForTesting
    static synchronized void internalDoLog4jPluginLoadingWorkaround( PluginRegistry pluginRegistry, Function<PluginRegistry,Boolean> checkIsShaded,
            Consumer<String> addPackageToPluginManager, Consumer<PluginRegistry> trickLog4j )
    {
        // Don't do the workaround if not shaded (for tests to work locally). When it's not shaded the Log4j2Plugins.dat file exist
        // and we won't find anything in the shaded package
        if ( checkIsShaded.apply( pluginRegistry ) )
        {
            // If we don't have any Log4j2Plugins.dat files on the classpath then we don't need to use reflection to fake that we have looked up the files.
            // No Log4j2Plugins.dat files will be found and Log4j will look in the package where it expects its plugins to be and not find
            // anything there either. It will then look in the packages we specify instead and find the plugins.
            if ( !pluginRegistry.loadFromMainClassLoader().isEmpty() || !pluginRegistry.loadFromPackage( PKG_UNSHADED_LOG4J ).isEmpty() )
            {
                pluginRegistry.clear();

                // Trick Log4j into thinking that it has already looked at the Log4j2Plugins.dat files in the classpath
                trickLog4j.accept( pluginRegistry );
            }

            // Make Log4j look through the packages where the plugins actually reside when shaded
            pluginRegistry.loadFromPackage( PKG_SHADED_LOG4J );
            pluginRegistry.loadFromPackage( PKG_OUR_PLUGINS );
            // The Log4j plugin handling is done in more than one place, adding the packages to PluginManger as well to avoid some ugly error messages
            addPackageToPluginManager.accept( PKG_SHADED_LOG4J );
            addPackageToPluginManager.accept( PKG_OUR_PLUGINS );
        }
    }

    private static void trickLog4jThatThereAreNoFiles( PluginRegistry pluginRegistry )
    {
        try
        {
            PrivilegedExceptionAction<Void> fakeLookedAtFiles = () ->
            {
                Field plugins = PluginRegistry.class.getDeclaredField( "pluginsByCategoryRef" );
                plugins.setAccessible( true );
                AtomicReference<Map<String,List<PluginType<?>>>> pluginsByCategoryRef = new AtomicReference<>();
                Map<String,List<PluginType<?>>> map = new HashMap<>();
                map.put( "dummy", new ArrayList<>() );
                pluginsByCategoryRef.set( map );
                plugins.set( pluginRegistry, pluginsByCategoryRef );
                return null;
            };
            AccessController.doPrivileged( fakeLookedAtFiles );
        }
        catch ( Exception e )
        {
            throw new LinkageError( "Cannot access " + PKG_SHADED_LOG4J, e );
        }
    }
}
