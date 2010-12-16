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

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.neo4j.helpers.Service;

public abstract class ServerExtension
{
    final String name;

    public ServerExtension( String name )
    {
        this.name = verifyName( name );
    }

    public ServerExtension()
    {
        this.name = verifyName( getClass().getSimpleName() );
    }

    static String verifyName( String name )
    {
        if ( name == null )
        {
            throw new IllegalArgumentException( "Name may not be null" );
        }
        try
        {
            if ( !URLEncoder.encode( name, "UTF-8" ).equals( name ) )
            {
                throw new IllegalArgumentException( "Name contained illegal characters" );
            }
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new Error( "UTF-8 should be supported", e );
        }
        return name;
    }

    @Override
    public String toString()
    {
        return "ServerExtension[" + name + "]";
    }

    static Iterable<ServerExtension> load()
    {
        return Service.load( ServerExtension.class );
    }

    protected void loadServerExtender( ServerExtender extender, Configuration serverConfig )
    {
        for ( ExtensionPoint extension : getDefaultExtensionPoints( serverConfig ) )
        {
            extender.addExtension( extension.forType(), extension );
        }
    }

    protected Collection<ExtensionPoint> getDefaultExtensionPoints( Configuration serverConfig )
    {
        List<ExtensionPoint> result = new ArrayList<ExtensionPoint>();
        for ( Method method : getClass().getMethods() )
        {
            ExtensionTarget target = method.getAnnotation( ExtensionTarget.class );
            if ( target != null )
            {
                result.add( ServerExtensionMethod.createFrom( this, method, target.value() ) );
            }
        }
        return result;
    }
}
