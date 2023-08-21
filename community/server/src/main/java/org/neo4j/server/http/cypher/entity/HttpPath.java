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
package org.neo4j.server.http.cypher.entity;

import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

public class HttpPath implements Path {
    private final List<Relationship> relationships;
    private final List<Node> nodes;

    public HttpPath(List<Node> nodes, List<Relationship> relationships) {
        this.relationships = relationships;
        this.nodes = nodes;
    }

    @Override
    public Node startNode() {
        return nodes.get(0);
    }

    @Override
    public Node endNode() {
        return nodes.get(nodes.size() - 1);
    }

    @Override
    public Relationship lastRelationship() {
        return relationships.get(relationships.size() - 1);
    }

    @Override
    public Iterable<Relationship> relationships() {
        return relationships;
    }

    @Override
    public Iterable<Relationship> reverseRelationships() {
        throw new NotImplementedException("HttpPath cannot reverse relationships");
    }

    @Override
    public Iterable<Node> nodes() {
        return nodes;
    }

    @Override
    public Iterable<Node> reverseNodes() {
        throw new NotImplementedException("HttpPath cannot reverse nodes");
    }

    @Override
    public int length() {
        return relationships.size();
    }

    @Override
    public Iterator<Entity> iterator() {
        return new Iterator<>() {
            Iterator<? extends Entity> current = nodes().iterator();
            Iterator<? extends Entity> next = relationships().iterator();

            @Override
            public boolean hasNext() {
                return current.hasNext();
            }

            @Override
            public Entity next() {
                try {
                    return current.next();
                } finally {
                    Iterator<? extends Entity> temp = current;
                    current = next;
                    next = temp;
                }
            }

            @Override
            public void remove() {
                next.remove();
            }
        };
    }
}
