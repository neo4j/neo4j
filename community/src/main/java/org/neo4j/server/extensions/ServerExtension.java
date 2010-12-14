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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.neo4j.helpers.Service;

public abstract class ServerExtension
{
    private final String name;

    public ServerExtension( String name )
    {
        this.name = name;
    }

    public ServerExtension()
    {
        this.name = getClass().getSimpleName();
    }

    public Collection<MediaExtender> getServerMediaExtenders( Configuration serverConfig )
    {
        List<MediaExtender> result = new ArrayList<MediaExtender>();
        for ( Method method : getClass().getMethods() )
        {
            ExtensionTarget discovery = method.getAnnotation( ExtensionTarget.class );
            if ( discovery != null )
            {
                result.add( ServerExtensionMethod.createFrom( this, method, discovery.value() ) );
            }
        }
        return result;
    }

    static Iterable<ServerExtension> load()
    {
        return Service.load( ServerExtension.class );
    }
}
