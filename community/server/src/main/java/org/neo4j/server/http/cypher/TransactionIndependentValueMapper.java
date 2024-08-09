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
package org.neo4j.server.http.cypher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.server.http.cypher.entity.HttpNode;
import org.neo4j.server.http.cypher.entity.HttpPath;
import org.neo4j.server.http.cypher.entity.HttpRelationship;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

public class TransactionIndependentValueMapper extends DefaultValueMapper {
    private final CachingWriter cachingWriter;

    public TransactionIndependentValueMapper(CachingWriter cachingWriter) {
        // transaction not needed
        super(null);
        this.cachingWriter = cachingWriter;
    }

    @Override
    public Object mapMap(MapValue value) {
        Map<Object, Object> map = new HashMap<>();
        value.foreach((k, v) -> {
            if (v instanceof NodeValue || v instanceof RelationshipValue) {
                try {
                    v.writeTo(cachingWriter);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                map.put(k, cachingWriter.getCachedObject());
            } else {
                map.put(k, v.map(this));
            }
        });
        return map;
    }

    @Override
    public List<?> mapSequence(SequenceValue value) {
        List<Object> list = new ArrayList<>(value.intSize());
        value.forEach(v -> {
            if (v instanceof NodeValue || v instanceof RelationshipValue) {
                try {
                    v.writeTo(cachingWriter);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                list.add(cachingWriter.getCachedObject());
            } else {
                list.add(v.map(this));
            }
        });
        return list;
    }

    @Override
    public Node mapNode(VirtualNodeValue value) {
        try {
            value.writeTo(cachingWriter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return (HttpNode) cachingWriter.getCachedObject();
    }

    @Override
    public Relationship mapRelationship(VirtualRelationshipValue value) {
        try {
            value.writeTo(cachingWriter);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return (HttpRelationship) cachingWriter.getCachedObject();
    }

    @Override
    public Path mapPath(VirtualPathValue value) {
        try {
            value.writeTo(cachingWriter);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return (HttpPath) cachingWriter.getCachedObject();
    }
}
