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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

class ServerExtensionMethod extends MediaExtender
{
    ServerExtensionMethod( String name, Class<?> discovery, ServerExtension extension,
            Method method, DataExtractor[] extractors )
    {
        super( discovery, name );
    }

    static ServerExtensionMethod createFrom( ServerExtension extension, Method method,
            Class<?> discovery )
    {
        ResultConverter result = ResultConverter.get( method.getReturnType() );
        Class<?>[] types = method.getParameterTypes();
        Annotation[][] annotations = method.getParameterAnnotations();
        SourceExtractor sourceExtractor = null;
        DataExtractor[] extractors = new DataExtractor[types.length];
        for ( int i = 0; i < types.length; i++ )
        {
            Description description = null;
            Parameter param = null;
            Source source = null;
            for ( Annotation annotation : annotations[i] )
            {
                if ( annotation instanceof Description )
                {
                    description = (Description) annotation;
                }
                else if ( annotation instanceof Parameter )
                {
                    param = (Parameter) annotation;
                }
                else if ( annotation instanceof Source )
                {
                    source = (Source) annotation;
                }
            }
            if ( param != null && source != null )
            {
                throw new IllegalStateException(
                        String.format(
                                "Method parameter %d of %s cannot be retrieved as both Parameter and Source",
                                Integer.valueOf( i ), method ) );
            }
            else if ( source != null )
            {
                if ( types[i] != discovery )
                {
                    throw new IllegalStateException(
                            "Source parameter type must equal the discovery type." );
                }
                if ( sourceExtractor != null )
                {
                    throw new IllegalStateException(
                            "Server Extension methods may have at most one Source parameter." );
                }
                extractors[i] = sourceExtractor = new SourceExtractor( source );
            }
            else if ( param != null )
            {
                extractors[i] = new ParameterExtractor( param );
            }
            else
            {
                throw new IllegalStateException(
                        "Parameters of Server Extension methods must be annotated as either Source or Parameter." );
            }
        }
        return new ServerExtensionMethod( nameOf( method ), discovery, extension, method,
                extractors );
    }

    private static String nameOf( Method method )
    {
        Name name = method.getAnnotation( Name.class );
        if ( name != null )
        {
            return name.value();
        }
        return method.getName(); // TODO: should we restructure the name?
    }

    private static abstract class DataExtractor
    {
    }

    private static class SourceExtractor extends DataExtractor
    {
        SourceExtractor( Source source )
        {
        }
    }

    private static class ParameterExtractor extends DataExtractor
    {
        ParameterExtractor( Parameter param )
        {
        }
    }
}
