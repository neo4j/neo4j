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
package org.neo4j.notifications;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.IndexHintException.IndexHintIndexType;

class NotificationDetailTest {
    @Test
    void shouldConstructNodeIndexDetails() {
        String detail =
                NotificationDetail.indexHint(EntityType.NODE, IndexHintIndexType.ANY, "person", "Person", "name");
        assertThat(detail).isEqualTo("index is: INDEX FOR (`person`:`Person`) ON (`person`.`name`)");
    }

    @Test
    void shouldConstructRelationshipIndexDetails() {
        String detail = NotificationDetail.indexHint(
                EntityType.RELATIONSHIP, IndexHintIndexType.ANY, "person", "Person", "name");
        assertThat(detail).isEqualTo("index is: INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`)");
    }

    @Test
    void shouldConstructCartesianProductDetailsSingular() {
        Set<String> idents = new HashSet<>();
        idents.add("n");
        String detail = NotificationDetail.cartesianProductDescription(idents);
        assertThat(detail).isEqualTo("identifier is: (n)");
    }

    @Test
    void shouldConstructCartesianProductDetails() {
        Set<String> idents = new TreeSet<>();
        idents.add("n");
        idents.add("node2");
        String detail = NotificationDetail.cartesianProductDescription(idents);
        assertThat(detail).isEqualTo("identifiers are: (n, node2)");
    }

    @Test
    void shouldConstructJoinHintDetailsSingular() {
        List<String> idents = new ArrayList<>();
        idents.add("n");
        String detail = NotificationDetail.joinKey(idents);
        assertThat(detail).isEqualTo("hinted join key identifier is: n");
    }

    @Test
    void shouldConstructJoinHintDetails() {
        List<String> idents = new ArrayList<>();
        idents.add("n");
        idents.add("node2");
        String detail = NotificationDetail.joinKey(idents);
        assertThat(detail).isEqualTo("hinted join key identifiers are: n, node2");
    }
}
