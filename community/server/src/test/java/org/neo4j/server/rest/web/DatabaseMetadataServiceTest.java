/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.rest.web;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;

public class DatabaseMetadataServiceTest
{
    @Test
    public void shouldAdvertiseRelationshipTyoesThatCurrentlyExistInTheDatabase() throws JsonParseException
    {
        HashSet<RelationshipType> fakeRelationshipTypes = new HashSet<RelationshipType>();
        fakeRelationshipTypes.add( DynamicRelationshipType.withName( "a" ) );
        fakeRelationshipTypes.add( DynamicRelationshipType.withName( "b" ) );
        fakeRelationshipTypes.add( DynamicRelationshipType.withName( "c" ) );

        Database database = mock( Database.class );
        AbstractGraphDatabase graph = mock( AbstractGraphDatabase.class );
        database.graph = graph;
        when( database.graph.getRelationshipTypes() ).thenReturn( fakeRelationshipTypes );
        DatabaseMetadataService service = new DatabaseMetadataService( database );

        Response response = service.getRelationshipTypes();

        assertEquals( 200, response.getStatus() );
        List<Map<String, Object>> jsonList = JsonHelper.jsonToList( response.getEntity()
                .toString() );
        assertEquals( 3, jsonList.size() );
    }
}
