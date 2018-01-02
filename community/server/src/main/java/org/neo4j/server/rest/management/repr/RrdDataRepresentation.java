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

import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;
import org.neo4j.server.rest.repr.ObjectRepresentation;
import org.neo4j.server.rest.repr.ValueRepresentation;
import org.rrd4j.core.FetchData;

public class RrdDataRepresentation extends ObjectRepresentation
{
    private final FetchData rrdData;

    public RrdDataRepresentation( FetchData rrdData )
    {
        super( "rrd-data" );
        this.rrdData = rrdData;
    }

    @Mapping( "start_time" )
    public ValueRepresentation getStartTime()
    {
        return ValueRepresentation.number( rrdData.getFirstTimestamp() );
    }

    @Mapping( "end_time" )
    public ValueRepresentation getEndTime()
    {
        return ValueRepresentation.number( rrdData.getLastTimestamp() );
    }

    @Mapping( "timestamps" )
    public ListRepresentation getTimestamps()
    {
        return ListRepresentation.numbers( rrdData.getTimestamps() );
    }

    @Mapping( "data" )
    public MappingRepresentation getDatasources()
    {
        return new MappingRepresentation( "datasources" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
                String[] dsNames = rrdData.getDsNames();
                for ( int i = 0, l = dsNames.length; i < l; i++ )
                {
                    serializer.putList( dsNames[i], ListRepresentation.numbers( rrdData.getValues( i ) ) );
                }
            }
        };
    }
}
