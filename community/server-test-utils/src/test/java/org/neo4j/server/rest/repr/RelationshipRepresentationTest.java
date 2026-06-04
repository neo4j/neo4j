/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.repr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.server.rest.repr.RepresentationTestAccess.serialize;
import static org.neo4j.server.rest.repr.RepresentationTestBase.NODE_URI_PATTERN;
import static org.neo4j.server.rest.repr.RepresentationTestBase.RELATIONSHIP_URI_PATTERN;
import static org.neo4j.server.rest.repr.RepresentationTestBase.assertUriMatches;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.neo4j.server.http.cypher.entity.HttpRelationship;

class RelationshipRepresentationTest {
    @Test
    void shouldHaveSelfLink() {
        assertUriMatches(RELATIONSHIP_URI_PATTERN, relrep(1234).selfUri());
    }

    @Test
    void shouldHaveType() {
        assertThat(relrep(1234).getType()).isNotNull();
    }

    @Test
    void shouldHaveStartNodeLink() {
        assertUriMatches(NODE_URI_PATTERN, relrep(1234).startNodeUri());
    }

    @Test
    void shouldHaveEndNodeLink() {
        assertUriMatches(NODE_URI_PATTERN, relrep(1234).endNodeUri());
    }

    @Test
    void shouldHavePropertiesLink() {
        assertUriMatches(RELATIONSHIP_URI_PATTERN + "/properties", relrep(1234).propertiesUri());
    }

    @Test
    void shouldHavePropertyLinkTemplate() {
        assertUriMatches(
                RELATIONSHIP_URI_PATTERN + "/properties/\\{key\\}", relrep(1234).propertyUriTemplate());
    }

    @Test
    void shouldSerialiseToMap() {
        Map<String, Object> repr = serialize(relrep(1234));
        assertThat(repr).isNotNull();
        verifySerialisation(repr);
    }

    private static RelationshipRepresentation relrep(long id) {
        return new RelationshipRepresentation(new HttpRelationship(
                "0",
                0,
                "0",
                0,
                "0",
                1,
                "LOVES",
                Collections.emptyMap(),
                false,
                (ignoredA, ignoredB) -> Optional.empty()));
    }

    static void verifySerialisation(Map<String, Object> relrep) {
        assertUriMatches(RELATIONSHIP_URI_PATTERN, relrep.get("self").toString());
        assertUriMatches(NODE_URI_PATTERN, relrep.get("start").toString());
        assertUriMatches(NODE_URI_PATTERN, relrep.get("end").toString());
        assertThat(relrep.get("type")).isNotNull();
        assertUriMatches(
                RELATIONSHIP_URI_PATTERN + "/properties",
                relrep.get("properties").toString());
        assertUriMatches(RELATIONSHIP_URI_PATTERN + "/properties/\\{key\\}", (String) relrep.get("property"));
        assertThat(relrep.get("data")).isNotNull();
        assertThat(relrep.get("metadata")).isNotNull();
        Map metadata = (Map) relrep.get("metadata");
        assertThat(metadata.get("type")).isNotNull();
        assertThat(((Number) metadata.get("id")).longValue()).isGreaterThanOrEqualTo(0);
    }
}
