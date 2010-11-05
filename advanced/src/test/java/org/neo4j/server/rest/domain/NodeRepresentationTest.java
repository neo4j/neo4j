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
import org.neo4j.server.rest.domain.NodeRepresentation;
import org.neo4j.server.rest.domain.PropertiesMap;

public class NodeRepresentationTest {
    private static final URI BASE_URI;
    static {
        try {
            BASE_URI = new URI("http://neo4j.org/");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldHaveSelfLink() {
        assertUriMatches(uriPattern(""), noderep(1234).selfUri());
    }

    @Test
    public void shouldHaveAllRelationshipsLink() {
        assertUriMatches(uriPattern("/relationships/all"), noderep(1234).allRelationshipsUri());
    }

    @Test
    public void shouldHaveIncomingRelationshipsLink() {
        assertUriMatches(uriPattern("/relationships/in"), noderep(1234).incomingRelationshipsUri());
    }

    @Test
    public void shouldHaveOutgoingRelationshipsLink() {
        assertUriMatches(uriPattern("/relationships/out"), noderep(1234).outgoingRelationshipsUri());
    }

    @Test
    public void shouldHaveAllTypedRelationshipsLinkTemplate() {
        assertUriMatches(uriPattern("/relationships/all/\\{-list\\|&\\|types\\}"), noderep(1234)
                .allTypedRelationshipsUriTemplate());
    }

    @Test
    public void shouldHaveIncomingTypedRelationshipsLinkTemplate() {
        assertUriMatches(uriPattern("/relationships/in/\\{-list\\|&\\|types\\}"), noderep(1234)
                .incomingTypedRelationshipsUriTemplate());
    }

    @Test
    public void shouldHaveOutgoingTypedRelationshipsLinkTemplate() {
        assertUriMatches(uriPattern("/relationships/out/\\{-list\\|&\\|types\\}"), noderep(1234)
                .outgoingTypedRelationshipsUriTemplate());
    }

    @Test
    public void shouldHaveRelationshipCreationLink() {
        assertUriMatches(uriPattern("/relationships"), noderep(1234).relationshipCreationUri());
    }

    @Test
    public void shouldHavePropertiesLink() {
        assertUriMatches(uriPattern("/properties"), noderep(1234).propertiesUri());
    }

    @Test
    public void shouldHavePropertyLinkTemplate() {
        assertUriMatches(uriPattern("/properties/\\{key\\}"), noderep(1234).propertyUriTemplate());
    }

    @Test
    public void shouldHaveTraverseLinkTemplate() {
        assertUriMatches(uriPattern("/traverse/\\{returnType\\}"), noderep(1234).traverseUriTemplate());
    }

    @Test
    public void shouldHavePropertiesData() {
        PropertiesMap data = noderep(1234).getProperties();
        assertNotNull(data);
    }

    @Test
    public void shouldSerialiseToMap() {
        Map<String, Object> repr = noderep(1234).serialize();
        assertNotNull(repr);
        verifySerialisation(repr);
    }

    private static void assertUriMatches(String expectedRegex, URI actualUri) {
        assertUriMatches(expectedRegex, actualUri.toString());
    }

    private static void assertUriMatches(String expectedRegex, String actualUri) {
        assertTrue("expected <" + expectedRegex + "> got <" + actualUri + ">", actualUri.matches(expectedRegex));
    }

    private static String uriPattern(String subPath) {
        return "http://.*/[0-9]+" + subPath;
    }

    private NodeRepresentation noderep(long id) {
        return new NodeRepresentation(BASE_URI, node(id));
    }

    private Node node(long id) {
        Node node = mock(Node.class);
        when(node.getId()).thenReturn(id);
        when(node.getPropertyKeys()).thenReturn(Collections.<String> emptySet());
        return node;
    }

    @SuppressWarnings("unchecked")
    public static void verifySerialisation(Map<String, Object> noderep) {
        assertUriMatches(uriPattern(""), (String) noderep.get("self"));
        assertUriMatches(uriPattern("/relationships"), (String) noderep.get("create_relationship"));
        assertUriMatches(uriPattern("/relationships/all"), (String) noderep.get("all_relationships"));
        assertUriMatches(uriPattern("/relationships/in"), (String) noderep.get("incoming_relationships"));
        assertUriMatches(uriPattern("/relationships/out"), (String) noderep.get("outgoing_relationships"));
        assertUriMatches(uriPattern("/relationships/all/\\{-list\\|&\\|types\\}"), (String) noderep
                .get("all_typed_relationships"));
        assertUriMatches(uriPattern("/relationships/in/\\{-list\\|&\\|types\\}"), (String) noderep
                .get("incoming_typed_relationships"));
        assertUriMatches(uriPattern("/relationships/out/\\{-list\\|&\\|types\\}"), (String) noderep
                .get("outgoing_typed_relationships"));
        assertUriMatches(uriPattern("/properties"), (String) noderep.get("properties"));
        assertUriMatches(uriPattern("/properties/\\{key\\}"), (String) noderep.get("property"));
        assertUriMatches( uriPattern( "/traverse/\\{returnType\\}" ),
                (String) noderep.get( "traverse" ) );
        assertNotNull((Map<String, Object>) noderep.get("data"));
    }
}
