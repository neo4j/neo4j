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

package org.neo4j.server.rest.domain;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.server.rest.domain.PropertiesMap;
import org.neo4j.server.rest.domain.RelationshipRepresentation;

public class RelationshipRepresentationTest {
    private static final URI BASE_URI;
    static {
        try {
            BASE_URI = new URI("http://neo4j.org/");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static final String NODE_URI_PATTERN = "http://.*/node/[0-9]+";
    private static final String RELATIONSHIP_URI_PATTERN = "http://.*/relationship/[0-9]+";

    @Test
    public void shouldHaveSelfLink() {
        assertUriMatches(RELATIONSHIP_URI_PATTERN, relrep(1234).selfUri());
    }

    @Test
    public void shouldHaveType() {
        assertNotNull(relrep(1234).getType());
    }

    @Test
    public void shouldHaveStartNodeLink() {
        assertUriMatches(NODE_URI_PATTERN, relrep(1234).startNodeUri());
    }

    @Test
    public void shouldHaveEndNodeLink() {
        assertUriMatches(NODE_URI_PATTERN, relrep(1234).endNodeUri());
    }

    @Test
    public void shouldHavePropertiesLink() {
        assertUriMatches(RELATIONSHIP_URI_PATTERN + "/properties", relrep(1234).propertiesUri());
    }

    @Test
    public void shouldHavePropertyLinkTemplate() {
        assertUriMatches(RELATIONSHIP_URI_PATTERN + "/properties/\\{key\\}", relrep(1234).propertyUriTemplate());
    }

    @Test
    public void shouldHavePropertiesData() {
        RelationshipRepresentation relrep = new RelationshipRepresentation(BASE_URI, relationship(1234));
        PropertiesMap data = relrep.getProperties();
        assertNotNull(data);
    }

    @Test
    public void shouldSerialiseToMap() {
        RelationshipRepresentation relrep = new RelationshipRepresentation(BASE_URI, relationship(1234));
        Map<String, Object> repr = relrep.serialize();
        assertNotNull(repr);
        verifySerialisation(repr);
    }

    private static void assertUriMatches(String expectedRegex, URI actualUri) {
        assertUriMatches(expectedRegex, actualUri.toString());
    }

    private static void assertUriMatches(String expectedRegex, String actualUri) {
        assertTrue("expected <" + expectedRegex + "> got <" + actualUri + ">", actualUri.matches(expectedRegex));
    }

    private RelationshipRepresentation relrep(long id) {
        return new RelationshipRepresentation(BASE_URI, relationship(id));
    }

    private Relationship relationship(long id) {
        Node startNode = mock(Node.class);
        when(startNode.getId()).thenReturn(0L);

        Node endNode = mock(Node.class);
        when(endNode.getId()).thenReturn(1L);

        RelationshipType type = mock(RelationshipType.class);
        when(type.name()).thenReturn("LOVES");

        Relationship relationship = mock(Relationship.class);
        when(relationship.getId()).thenReturn(id);
        when(relationship.getPropertyKeys()).thenReturn(Collections.<String> emptySet());
        when(relationship.getStartNode()).thenReturn(startNode);
        when(relationship.getEndNode()).thenReturn(endNode);
        when(relationship.getType()).thenReturn(type);

        return relationship;
    }

    @SuppressWarnings("unchecked")
    public static void verifySerialisation(Map<String, Object> relrep) {
        assertUriMatches(RELATIONSHIP_URI_PATTERN, (String) relrep.get("self"));
        assertUriMatches(NODE_URI_PATTERN, (String) relrep.get("start"));
        assertUriMatches(NODE_URI_PATTERN, (String) relrep.get("end"));
        assertNotNull((String) relrep.get("type"));
        assertUriMatches(RELATIONSHIP_URI_PATTERN + "/properties", (String) relrep.get("properties"));
        assertUriMatches(RELATIONSHIP_URI_PATTERN + "/properties/\\{key\\}", (String) relrep.get("property"));
        assertNotNull((Map<String, Object>) relrep.get("data"));
    }
}
