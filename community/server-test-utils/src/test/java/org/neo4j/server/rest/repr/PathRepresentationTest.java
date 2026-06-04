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
import static org.neo4j.test.mockito.mock.GraphMock.node;
import static org.neo4j.test.mockito.mock.GraphMock.path;
import static org.neo4j.test.mockito.mock.GraphMock.relationship;
import static org.neo4j.test.mockito.mock.Link.link;
import static org.neo4j.test.mockito.mock.Properties.properties;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

class PathRepresentationTest {

    @Test
    void shouldHaveLength() {
        assertThat(pathrep().length()).isNotNull();
    }

    @Test
    void shouldHaveStartNodeLink() {
        assertUriMatches(NODE_URI_PATTERN, pathrep().startNode());
    }

    @Test
    void shouldHaveEndNodeLink() {
        assertUriMatches(NODE_URI_PATTERN, pathrep().endNode());
    }

    @Test
    void shouldHaveNodeList() {
        assertThat(pathrep().nodes()).isNotNull();
    }

    @Test
    void shouldHaveRelationshipList() {
        assertThat(pathrep().relationships()).isNotNull();
    }

    @Test
    void shouldHaveDirectionList() {
        assertThat(pathrep().directions()).isNotNull();
    }

    @Test
    void shouldSerialiseToMap() {
        Map<String, Object> repr = serialize(pathrep());
        assertThat(repr).isNotNull();
        verifySerialisation(repr);
    }

    /*
     * Construct a sample path representation of the form:
     *
     *     (A)-[:LOVES]->(B)<-[:HATES]-(C)-[:KNOWS]->(D)
     *
     * This contains two forward relationships and one backward relationship
     * which is represented in the "directions" value of the output. We should
     * therefore see something like the following:
     *
     * {
     *     "length" : 3,
     *     "start" : "http://neo4j.org/node/0",
     *     "end" : "http://neo4j.org/node/3",
     *     "nodes" : [
     *         "http://neo4j.org/node/0", "http://neo4j.org/node/1",
     *         "http://neo4j.org/node/2", "http://neo4j.org/node/3"
     *     ],
     *     "relationships" : [
     *         "http://neo4j.org/relationship/17",
     *         "http://neo4j.org/relationship/18",
     *         "http://neo4j.org/relationship/19"
     *     ],
     *     "directions" : [ "->", "<-", "->" ]
     * }
     *
     */
    private static PathRepresentation<Path> pathrep() {
        Node a = node(0, properties());
        Node b = node(1, properties());
        Node c = node(2, properties());
        Node d = node(3, properties());

        Relationship ab = relationship(17, a, "LOVES", b);
        Relationship cb = relationship(18, c, "HATES", b);
        Relationship cd = relationship(19, c, "KNOWS", d);

        return new PathRepresentation<>(path(a, link(ab, b), link(cb, c), link(cd, d)));
    }

    public static void verifySerialisation(Map<String, Object> pathrep) {
        assertThat(pathrep.get("length")).isNotNull();
        int length = Integer.parseInt(pathrep.get("length").toString());

        assertUriMatches(NODE_URI_PATTERN, pathrep.get("start").toString());
        assertUriMatches(NODE_URI_PATTERN, pathrep.get("end").toString());

        assertThat(pathrep.get("nodes"))
                .asList()
                .hasSize(length + 1)
                .allSatisfy(node -> assertUriMatches(NODE_URI_PATTERN, node.toString()));

        assertThat(pathrep.get("relationships"))
                .asList()
                .hasSize(length)
                .allSatisfy(rel -> assertUriMatches(RELATIONSHIP_URI_PATTERN, rel.toString()));

        assertThat(pathrep.get("directions")).asList().hasSize(length).containsExactly("->", "<-", "->");
    }
}
