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

package org.neo4j.server.extensions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.server.rest.repr.ExtensionInjector;
import org.neo4j.server.rest.repr.ExtensionUri;

public class ExtensionManager implements ExtensionInjector
{
    private static final Logger log = Logger.getLogger( ExtensionManager.class );
    private final Map<Class<?>, Map<String, ExtensionUri>> mediaExtenders = newExtendersMap();

    public ExtensionManager( Configuration serverConfig )
    {
        LOADING_EXTENSIONS: for ( ServerExtension extension : ServerExtension.load() )
        {
            final Collection<MediaExtender> extenders;
            try
            {
                extenders = extension.getServerMediaExtenders( serverConfig );
            }
            catch ( Exception e )
            {
                error( extension, "Exception while loading extension", e );
                continue;
            }
            Map<Class<?>, Map<String, MediaExtender>> additions = newExtendersMap();
            for ( MediaExtender extender : extenders )
            {
                Map<String, MediaExtender> map = additions.get( extender.forType() );
                if ( map == null )
                {
                    error( extension, "Cannot extend type: " + extender.forType(), null );
                    continue LOADING_EXTENSIONS;
                }
                if ( map.put( extender.name(), extender ) != null )
                {
                    error( extension, "Multiple definitions of \"" + extender.name() + "\" for "
                                      + extender.forType().getSimpleName(), null );
                    continue LOADING_EXTENSIONS;
                }
                if ( mediaExtenders.get( extender.forType() ).containsKey( extender.name() ) )
                {
                    error( extension, "The extension \"" + extender.name()
                                      + "\" is already defined for "
                                      + extender.forType().getSimpleName(), null );
                    continue LOADING_EXTENSIONS;
                }
            }
            for ( Map.Entry<Class<?>, Map<String, MediaExtender>> add : additions.entrySet() )
            {
                Map<String, MediaExtender> map = add.getValue();
                Map<String, ExtensionUri> uris = mediaExtenders.get( add.getKey() );
                for ( Map.Entry<String, MediaExtender> entry : map.entrySet() )
                {
                    uris.put( entry.getKey(), /*TODO: compute extension uri:*/null );
                }
            }
        }
    }

    private void error( ServerExtension extension, String message, Exception cause )
    {
        if ( cause == null )
        {
            log.warn( "Failed to load server extension: " + extension + "\n" + message );
        }
        else
        {
            log.warn( "Failed to load server extension: " + extension + "\n" + message, cause );
        }
    }

    private static final Class<?>[] EXTENSIBLE_TYPES = { GraphDatabaseService.class, Node.class,
            Relationship.class, Path.class };

    private static <T> Map<Class<?>, Map<String, T>> newExtendersMap()
    {
        @SuppressWarnings( "unchecked" ) Map<Class<?>, Map<String, T>> extendersMap = new HashMap();
        for ( Class<?> type : EXTENSIBLE_TYPES )
        {
            extendersMap.put( type, new HashMap<String, T>() );
        }
        return Collections.unmodifiableMap( extendersMap );
    }

    @Override
    public Map<String, ExtensionUri> getExensionsFor( Class<?> type )
    {
        System.out.println( "I'm here! (don't be a gurly gurl)" );
        if ( /*implementation not done yet:*/true ) return null;
        Map<String, ExtensionUri> extenders = mediaExtenders.get( type );
        if ( extenders == null || extenders.isEmpty() ) return null;
        return Collections.unmodifiableMap( extenders );
    }
}
