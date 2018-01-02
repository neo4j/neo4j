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
package org.neo4j.server.rest.management.repr;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;
import org.neo4j.server.rest.management.AdvertisableService;

public class ServerRootRepresentation extends MappingRepresentation
{
    private HashMap<String, String> services = new HashMap<String, String>();

    public ServerRootRepresentation( URI baseUri, Iterable<AdvertisableService> advertisableServices )
    {
        super( "services" );
        for ( AdvertisableService svc : advertisableServices )
        {
            services.put( svc.getName(), baseUri.toString() + svc.getServerPath() );
        }
    }

    public Map<String, Map<String, String>> serialize()
    {
        HashMap<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
        result.put( "services", services );
        return result;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        MappingRepresentation apa = new MappingRepresentation( "services" )
        {

            @Override
            protected void serialize( MappingSerializer serializer )
            {
                for ( Map.Entry<String, String> entry : services.entrySet() )
                {
                    serializer.putString( entry.getKey(), entry.getValue() );
                }
            }
        };

        serializer.putMapping( "services", apa );
    }
}
