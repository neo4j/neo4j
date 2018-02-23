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
package org.neo4j.server.rest.repr;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.server.rest.repr.RepresentationTestAccess.serialize;
import static org.neo4j.server.rest.repr.RepresentationTestBase.assertUriMatches;
import static org.neo4j.server.rest.repr.RepresentationTestBase.uriPattern;
import static org.neo4j.test.mockito.mock.GraphMock.node;
import static org.neo4j.test.mockito.mock.Properties.properties;

public class NodeRepresentationTest
{
    @Test
    public void shouldHaveSelfLink()
    {
        assertUriMatches( uriPattern( "" ), noderep( 1234 ).selfUri() );
    }

    @Test
    public void shouldHaveAllRelationshipsLink()
    {
        assertUriMatches( uriPattern( "/relationships/all" ), noderep( 1234 ).allRelationshipsUri() );
    }

    @Test
    public void shouldHaveIncomingRelationshipsLink()
    {
        assertUriMatches( uriPattern( "/relationships/in" ), noderep( 1234 ).incomingRelationshipsUri() );
    }

    @Test
    public void shouldHaveOutgoingRelationshipsLink()
    {
        assertUriMatches( uriPattern( "/relationships/out" ), noderep( 1234 ).outgoingRelationshipsUri() );
    }

    @Test
    public void shouldHaveAllTypedRelationshipsLinkTemplate()
    {
        assertUriMatches( uriPattern( "/relationships/all/\\{-list\\|&\\|types\\}" ),
                noderep( 1234 ).allTypedRelationshipsUriTemplate() );
    }

    @Test
    public void shouldHaveIncomingTypedRelationshipsLinkTemplate()
    {
        assertUriMatches( uriPattern( "/relationships/in/\\{-list\\|&\\|types\\}" ),
                noderep( 1234 ).incomingTypedRelationshipsUriTemplate() );
    }

    @Test
    public void shouldHaveOutgoingTypedRelationshipsLinkTemplate()
    {
        assertUriMatches( uriPattern( "/relationships/out/\\{-list\\|&\\|types\\}" ),
                noderep( 1234 ).outgoingTypedRelationshipsUriTemplate() );
    }

    @Test
    public void shouldHaveRelationshipCreationLink()
    {
        assertUriMatches( uriPattern( "/relationships" ), noderep( 1234 ).relationshipCreationUri() );
    }

    @Test
    public void shouldHavePropertiesLink()
    {
        assertUriMatches( uriPattern( "/properties" ), noderep( 1234 ).propertiesUri() );
    }

    @Test
    public void shouldHavePropertyLinkTemplate()
    {
        assertUriMatches( uriPattern( "/properties/\\{key\\}" ), noderep( 1234 ).propertyUriTemplate() );
    }

    @Test
    public void shouldHaveTraverseLinkTemplate()
    {
        assertUriMatches( uriPattern( "/traverse/\\{returnType\\}" ), noderep( 1234 ).traverseUriTemplate() );
    }

    @Test
    public void shouldSerialiseToMap()
    {
        Map<String, Object> repr = serialize( noderep( 1234 ) );
        assertNotNull( repr );
        verifySerialisation( repr );
    }

    @Test
    public void shouldHaveLabelsLink()
    {
        assertUriMatches( uriPattern( "/labels" ), noderep( 1234 ).labelsUriTemplate() );
    }

    private NodeRepresentation noderep( long id )
    {
        return new NodeRepresentation( node( id, properties(), "Label" ) );
    }

    public static void verifySerialisation( Map<String, Object> noderep )
    {
        assertUriMatches( uriPattern( "" ), noderep.get( "self" )
                .toString() );
        assertUriMatches( uriPattern( "/relationships" ), noderep.get( "create_relationship" )
                .toString() );
        assertUriMatches( uriPattern( "/relationships/all" ), noderep.get( "all_relationships" )
                .toString() );
        assertUriMatches( uriPattern( "/relationships/in" ), noderep.get( "incoming_relationships" )
                .toString() );
        assertUriMatches( uriPattern( "/relationships/out" ), noderep.get( "outgoing_relationships" )
                .toString() );
        assertUriMatches( uriPattern( "/relationships/all/\\{-list\\|&\\|types\\}" ),
                (String) noderep.get( "all_typed_relationships" ) );
        assertUriMatches( uriPattern( "/relationships/in/\\{-list\\|&\\|types\\}" ),
                (String) noderep.get( "incoming_typed_relationships" ) );
        assertUriMatches( uriPattern( "/relationships/out/\\{-list\\|&\\|types\\}" ),
                (String) noderep.get( "outgoing_typed_relationships" ) );
        assertUriMatches( uriPattern( "/properties" ), noderep.get( "properties" )
                .toString() );
        assertUriMatches( uriPattern( "/properties/\\{key\\}" ), (String) noderep.get( "property" ) );
        assertUriMatches( uriPattern( "/traverse/\\{returnType\\}" ), (String) noderep.get( "traverse" ) );
        assertUriMatches( uriPattern( "/labels" ), (String) noderep.get( "labels" ) );
        assertNotNull( noderep.get( "data" ) );
        Map metadata = (Map) noderep.get( "metadata" );
        List labels = (List) metadata.get( "labels" );
        assertTrue( labels.isEmpty() || labels.equals( asList( "Label" ) ) );
        assertTrue( ( (Number) metadata.get("id") ).longValue() >= 0 );
    }
}
