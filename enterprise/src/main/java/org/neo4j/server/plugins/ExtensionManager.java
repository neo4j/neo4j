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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.ExtensionInjector;
import org.neo4j.server.rest.repr.ExtensionPointRepresentation;
import org.neo4j.server.rest.repr.Representation;

public final class ExtensionManager implements ExtensionInjector, ExtensionInvocator
{
    private static final Logger log = Logger.getLogger( ExtensionManager.class );
    private final Map<String/*name*/, ServerExtender> extensions = new HashMap<String, ServerExtender>();

    public ExtensionManager( Configuration serverConfig )
    {
        Map<String, Pair<ServerPlugin, ServerExtender>> extensions = new HashMap<String, Pair<ServerPlugin, ServerExtender>>();
        for ( ServerPlugin plugin : ServerPlugin.load() )
        {
            final ServerExtender extender = new ServerExtender();
            try
            {
                plugin.loadServerExtender( extender, serverConfig );
            }
            catch ( Exception ex )
            {
                log.warn( "Failed to load plugin: " + plugin, ex );
                continue;
            }
            catch ( LinkageError err )
            {
                log.warn( "Failed to load plugin: " + plugin, err );
                continue;
            }
            Pair<ServerPlugin, ServerExtender> old = extensions.put( plugin.name, Pair.of(
                    plugin, extender ) );
            if ( old != null )
            {
                log.warn( String.format(
                        "Extension naming conflict \"%s\" between \"%s\" and \"%s\"",
                        plugin.name, old.first().getClass(), plugin.getClass() ) );
            }
        }
        for ( Pair<ServerPlugin, ServerExtender> extension : extensions.values() )
        {
            this.extensions.put( extension.first().name, extension.other() );
        }
    }

    @Override
    public Map<String, List<String>> getExensionsFor( Class<?> type )
    {
        Map<String, List<String>> result = new HashMap<String, List<String>>();
        for ( Map.Entry<String, ServerExtender> extension : extensions.entrySet() )
        {
            List<String> methods = new ArrayList<String>();
            for ( ExtensionPoint method : extension.getValue().getExtensionsFor( type ) )
            {
                methods.add( method.name() );
            }
            if ( !methods.isEmpty() ) result.put( extension.getKey(), methods );
        }
        return result;
    }

    private ExtensionPoint extension( String name, Class<?> type, String method )
            throws ExtensionLookupException
    {
        ServerExtender extender = extensions.get( name );
        if ( extender == null )
        {
            throw new ExtensionLookupException( "No such ServerPlugin: \"" + name + "\"" );
        }
        return extender.getExtensionPoint( type, method );
    }

    @Override
    public ExtensionPointRepresentation describe( String name, Class<?> type, String method )
            throws ExtensionLookupException
    {
        return extension( name, type, method ).descibe();
    }

    @Override
    public List<ExtensionPointRepresentation> describeAll( String name )
            throws ExtensionLookupException
    {
        ServerExtender extender = extensions.get( name );
        if ( extender == null )
        {
            throw new ExtensionLookupException( "No such ServerPlugin: \"" + name + "\"" );
        }
        List<ExtensionPointRepresentation> result = new ArrayList<ExtensionPointRepresentation>();
        for ( ExtensionPoint extension : extender.all() )
        {
            result.add( extension.descibe() );
        }
        return result;
    }

    @Override
    public <T> Representation invoke( AbstractGraphDatabase graphDb, String name, Class<T> type,
            String method, T context, ParameterList params ) throws ExtensionLookupException,
            BadInputException, ExtensionInvocationFailureException, BadExtensionInvocationException
    {
        ExtensionPoint extension = extension( name, type, method );
        try
        {
            return extension.invoke( graphDb, context, params );
        }
        catch ( BadInputException e )
        {
            throw e;
        }
        catch ( BadExtensionInvocationException e )
        {
            throw e;
        }
        catch ( ExtensionInvocationFailureException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new ExtensionInvocationFailureException( e );
        }
    }

    @Override
    public Set<String> extensionNames()
    {
        return Collections.unmodifiableSet( extensions.keySet() );
    }
}
