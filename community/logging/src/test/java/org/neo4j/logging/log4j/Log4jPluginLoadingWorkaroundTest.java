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

import org.apache.logging.log4j.core.config.plugins.util.PluginRegistry;
import org.apache.logging.log4j.core.config.plugins.util.PluginType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.log4j.Log4jPluginLoadingWorkaround.PKG_OUR_PLUGINS;
import static org.neo4j.logging.log4j.Log4jPluginLoadingWorkaround.PKG_SHADED_LOG4J;
import static org.neo4j.logging.log4j.Log4jPluginLoadingWorkaround.PKG_UNSHADED_LOG4J;

class Log4jPluginLoadingWorkaroundTest
{
    private final PluginRegistry pluginRegistry = mock( PluginRegistry.class );
    private final List<String> packagesRegisteredWithPluginManager = new ArrayList<>();
    private final AtomicBoolean trickCalled = new AtomicBoolean( false );

    @Test
    void shouldDoNothingIfNotShaded()
    {
        Log4jPluginLoadingWorkaround.internalDoLog4jPluginLoadingWorkaround( pluginRegistry, x -> false, packagesRegisteredWithPluginManager::add,
                x -> trickCalled.set( true ) );

        verifyNoMoreInteractions( pluginRegistry );
        assertThat( trickCalled.get() ).isFalse();
        assertThat( packagesRegisteredWithPluginManager ).isEmpty();
    }

    @Test
    void shouldOnlyAddPackagesIfNoOtherLog4jFound()
    {
        when( pluginRegistry.loadFromMainClassLoader() ).thenReturn( new HashMap<>() );
        when( pluginRegistry.loadFromPackage( PKG_UNSHADED_LOG4J ) ).thenReturn( new HashMap<>() );

        Log4jPluginLoadingWorkaround.internalDoLog4jPluginLoadingWorkaround( pluginRegistry, x -> true, packagesRegisteredWithPluginManager::add,
                x -> trickCalled.set( true ) );

        verify( pluginRegistry ).loadFromPackage( PKG_UNSHADED_LOG4J );
        verify( pluginRegistry ).loadFromPackage( PKG_SHADED_LOG4J );
        verify( pluginRegistry ).loadFromPackage( PKG_OUR_PLUGINS );
        verify( pluginRegistry ).loadFromMainClassLoader();
        verifyNoMoreInteractions( pluginRegistry );
        assertThat( trickCalled.get() ).isFalse();
        assertThat( packagesRegisteredWithPluginManager ).containsExactlyInAnyOrder( PKG_SHADED_LOG4J, PKG_OUR_PLUGINS );
    }

    @Test
    void shouldFakeNoFilesIfOtherLog4jPluginsFileFound()
    {
        Map<String,List<PluginType<?>>> mapWithFoundLog4j = new HashMap<>();
        mapWithFoundLog4j.put( "key", new ArrayList<>() );
        when( pluginRegistry.loadFromMainClassLoader() ).thenReturn( mapWithFoundLog4j );

        Log4jPluginLoadingWorkaround.internalDoLog4jPluginLoadingWorkaround( pluginRegistry, x -> true, packagesRegisteredWithPluginManager::add,
                x -> trickCalled.set( true ) );

        verify( pluginRegistry ).loadFromPackage( PKG_SHADED_LOG4J );
        verify( pluginRegistry ).loadFromPackage( PKG_OUR_PLUGINS );
        verify( pluginRegistry ).loadFromMainClassLoader();
        verify( pluginRegistry ).clear();
        verifyNoMoreInteractions( pluginRegistry );
        assertThat( trickCalled.get() ).isTrue();
        assertThat( packagesRegisteredWithPluginManager ).containsExactlyInAnyOrder( PKG_SHADED_LOG4J, PKG_OUR_PLUGINS );
    }

    @Test
    void shouldFakeNoFilesIfOtherLog4jPackageFound()
    {
        Map<String,List<PluginType<?>>> mapWithFoundLog4j = new HashMap<>();
        mapWithFoundLog4j.put( "key", new ArrayList<>() );
        when( pluginRegistry.loadFromMainClassLoader() ).thenReturn( new HashMap<>() );
        when( pluginRegistry.loadFromPackage( PKG_UNSHADED_LOG4J ) ).thenReturn( mapWithFoundLog4j );

        Log4jPluginLoadingWorkaround.internalDoLog4jPluginLoadingWorkaround( pluginRegistry, x -> true, packagesRegisteredWithPluginManager::add,
                x -> trickCalled.set( true ) );

        verify( pluginRegistry ).loadFromPackage( PKG_UNSHADED_LOG4J );
        verify( pluginRegistry ).loadFromPackage( PKG_SHADED_LOG4J );
        verify( pluginRegistry ).loadFromPackage( PKG_OUR_PLUGINS );
        verify( pluginRegistry ).loadFromMainClassLoader();
        verify( pluginRegistry ).clear();
        verifyNoMoreInteractions( pluginRegistry );
        assertThat( trickCalled.get() ).isTrue();
        assertThat( packagesRegisteredWithPluginManager ).containsExactlyInAnyOrder( PKG_SHADED_LOG4J, PKG_OUR_PLUGINS );
    }
}
