/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.server.plugins;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.NestingIterable;

public final class ServerExtender
{
    @SuppressWarnings( "unchecked" )
    private final Map<Class<?>, Map<String, PluginPoint>> extensions = new HashMap();

    ServerExtender()
    {
        extensions.put( Node.class, new ConcurrentHashMap<String, PluginPoint>() );
        extensions.put( Relationship.class, new ConcurrentHashMap<String, PluginPoint>() );
        extensions.put( GraphDatabaseService.class, new ConcurrentHashMap<String, PluginPoint>() );
    }

    Iterable<PluginPoint> getExtensionsFor( Class<?> type )
    {
        Map<String, PluginPoint> ext = extensions.get( type );
        if ( ext == null ) return Collections.emptyList();
        return ext.values();
    }

    Iterable<PluginPoint> all()
    {
        return new NestingIterable<PluginPoint, Map<String, PluginPoint>>(
                extensions.values() )
        {
            @Override
            protected Iterator<PluginPoint> createNestedIterator(
                    Map<String, PluginPoint> item )
            {
                return item.values().iterator();
            }
        };
    }

    PluginPoint getExtensionPoint( Class<?> type, String method )
            throws ExtensionLookupException
    {
        Map<String, PluginPoint> ext = extensions.get( type );
        PluginPoint plugin = null;
        if ( ext != null )
        {
            plugin = ext.get( method );
        }
        if ( plugin == null )
        {
            throw new ExtensionLookupException( "No plugin \"" + method + "\" for " + type );
        }
        return plugin;
    }

    void addExtension( Class<?> type, PluginPoint plugin )
    {
        Map<String, PluginPoint> ext = extensions.get( type );
        if ( ext == null ) throw new IllegalStateException( "Cannot extend " + type );
        add( ext, plugin );
    }

    public void addGraphDatabaseExtensions( PluginPoint plugin )
    {
        add( extensions.get( GraphDatabaseService.class ), plugin );
    }

    public void addNodeExtensions( PluginPoint plugin )
    {
        add( extensions.get( Node.class ), plugin );
    }

    public void addRelationshipExtensions( PluginPoint plugin )
    {
        add( extensions.get( Relationship.class ), plugin );
    }

    private static void add( Map<String, PluginPoint> extensions, PluginPoint plugin )
    {
        if ( extensions.get( plugin.name() ) != null )
        {
            throw new IllegalArgumentException(
                    "This plugin already has an plugin point with the name \""
                            + plugin.name() + "\"" );
        }
        extensions.put( plugin.name(), plugin );
    }
}
