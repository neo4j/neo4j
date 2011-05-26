/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.rest.repr;

import java.util.Map;

public class BatchOperationRepresentation extends ObjectRepresentation 
{
    
    // TODO: Lab project. This should be defined together with other representation types.
    private static final RepresentationType TYPE = new RepresentationType( "batch-operation" );
    
    private final String from;
    private final Integer id;
    private final String responseData;
    private final Map<String, Object> headers;
    private final static String[] INCLUDE_HEADERS = {"Location"};
    
    public BatchOperationRepresentation( Integer id, String from, String responseData, Map<String, Object> headers )
    {
        super( TYPE );
        this.from = from;
        this.id = id;
        this.headers = headers;
        this.responseData = responseData;
    }

    @Mapping( "from" )
    public ValueRepresentation from() {
        return ValueRepresentation.string( from );
    }
    
    @Mapping( "body" )
    public ValueRepresentation responseData()
    {
        return ValueRepresentation.string( responseData );
    }
    
    @Override
    protected void extraData( MappingSerializer serializer )
    {
        if(id != null) {
            serializer.putNumber( "id", id );
        }
        
        for(String header : INCLUDE_HEADERS) {
            if(headers.containsKey( header )) {
                serializer.putString( header.toLowerCase(), headers.get( header ).toString() );
            }
        }
        
    }
    
    public static ListRepresentation list( Iterable<BatchOperationRepresentation> ops )
    {
        return new ListRepresentation( TYPE, ops );
    }
}
