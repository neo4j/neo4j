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
package org.neo4j.bolt.negotiation.version.expression;

import org.neo4j.bolt.negotiation.version.ProtocolVersion;

public interface ProtocolVersionExpression {

    boolean matches(ProtocolVersion version);

    default ProtocolVersionExpression inv() {
        return new NotExpression(this);
    }

    static ProtocolVersionExpression any() {
        return AnyExpression.getInstance();
    }

    static ProtocolVersionExpression none() {
        return NoneExpression.getInstance();
    }

    static ProtocolVersionExpression not(ProtocolVersionExpression expression) {
        return new NotExpression(expression);
    }

    default ProtocolVersionExpression and(ProtocolVersionExpression other) {
        return new AndExpression(this, other);
    }

    default ProtocolVersionExpression or(ProtocolVersionExpression other) {
        return new OrExpression(this, other);
    }

    static ProtocolVersionExpression equals(ProtocolVersion version) {
        return new EqualExpression(version);
    }

    static ProtocolVersionExpression equals(int major, int minor) {
        return equals(new ProtocolVersion(major, minor));
    }

    static ProtocolVersionExpression newerThan(ProtocolVersion version) {
        return new NewerThanExpression(version);
    }

    static ProtocolVersionExpression newerThan(int major, int minor) {
        return newerThan(new ProtocolVersion(major, minor));
    }

    static ProtocolVersionExpression newerThanOrEqualTo(ProtocolVersion version) {
        return newerThan(version).or(equals(version));
    }

    static ProtocolVersionExpression newerThanOrEqualTo(int major, int minor) {
        return newerThanOrEqualTo(new ProtocolVersion(major, minor));
    }

    static ProtocolVersionExpression olderThan(ProtocolVersion version) {
        return new OlderThanExpression(version);
    }

    static ProtocolVersionExpression olderThan(int major, int minor) {
        return olderThan(new ProtocolVersion(major, minor));
    }

    static ProtocolVersionExpression olderThanOrEqualTo(ProtocolVersion version) {
        return olderThan(version).or(equals(version));
    }

    static ProtocolVersionExpression olderThanOrEqualTo(int major, int minor) {
        return olderThanOrEqualTo(new ProtocolVersion(major, minor));
    }
}
