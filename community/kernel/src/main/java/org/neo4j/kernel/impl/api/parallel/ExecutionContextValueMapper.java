/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.parallel;

import org.neo4j.kernel.impl.util.NodeEntityWrappingNodeValue;
import org.neo4j.kernel.impl.util.RelationshipEntityWrappingValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

public class ExecutionContextValueMapper extends ValueMapper.JavaMapper {
    @Override
    public Object mapPath(VirtualPathValue value) {
        throw new UnsupportedOperationException(
                "Submitting Path to a function is not supported during parallel query execution.");
    }

    @Override
    public Object mapNode(VirtualNodeValue value) {
        if (value instanceof NodeEntityWrappingNodeValue wrapper) {
            return wrapper.getEntity();
        }

        return new ExecutionContextNode(value.id());
    }

    @Override
    public Object mapRelationship(VirtualRelationshipValue value) {
        if (value instanceof RelationshipEntityWrappingValue wrapper) {
            return wrapper.getEntity();
        }
        return new ExecutionContextRelationship(value.id());
    }
}
