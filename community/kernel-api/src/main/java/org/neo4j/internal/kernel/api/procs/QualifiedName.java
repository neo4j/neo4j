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
package org.neo4j.internal.kernel.api.procs;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.apache.commons.lang3.StringUtils.EMPTY;

import java.util.Arrays;
import org.neo4j.internal.helpers.collection.Iterables;

public class QualifiedName {
    private final String[] namespace;
    private final String name;
    private final String description;

    public QualifiedName(String[] namespace, String name) {
        this.namespace = namespace;
        this.name = name;
        this.description = buildDescription(namespace, name);
    }

    public QualifiedName(String name) {
        this(EMPTY_STRING_ARRAY, name);
    }

    public QualifiedName(String ns1, String name) {
        this(new String[] {ns1}, name);
    }

    public QualifiedName(String ns1, String ns2, String name) {
        this(new String[] {ns1, ns2}, name);
    }

    public QualifiedName(String ns1, String ns2, String ns3, String name) {
        this(new String[] {ns1, ns2, ns3}, name);
    }

    public QualifiedName(String ns1, String ns2, String ns3, String ns4, String name) {
        this(new String[] {ns1, ns2, ns3, ns4}, name);
    }

    public String[] namespace() {
        return namespace;
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        QualifiedName that = (QualifiedName) o;
        return Arrays.equals(namespace, that.namespace) && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(namespace) + name.hashCode();
    }

    private static String buildDescription(String[] namespace, String name) {
        var strNamespace = namespace.length > 0 ? Iterables.toString(asList(namespace), ".") + "." : EMPTY;
        return String.format("%s%s", strNamespace, name);
    }
}
