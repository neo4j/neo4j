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
package org.neo4j.internal.kernel.api.helpers.traversal.productgraph;

import java.util.function.IntPredicate;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.neo4j.internal.kernel.api.RelationshipDataReader;

public interface RelationshipPredicate {
    static Predicate<RelationshipDataReader> onId(LongPredicate idPredicate) {
        return (rel) -> idPredicate.test(rel.relationshipReference());
    }

    static Predicate<RelationshipDataReader> onSource(LongPredicate idPredicate) {
        return (rel) -> idPredicate.test(rel.sourceNodeReference());
    }

    static Predicate<RelationshipDataReader> onTarget(LongPredicate idPredicate) {
        return (rel) -> idPredicate.test(rel.targetNodeReference());
    }

    static Predicate<RelationshipDataReader> onType(IntPredicate idPredicate) {
        return (rel) -> idPredicate.test(rel.type());
    }
}
