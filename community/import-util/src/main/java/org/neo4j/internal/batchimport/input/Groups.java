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
package org.neo4j.internal.batchimport.input;

import static org.neo4j.util.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.ReadableGroups;

/**
 * Mapping from name to {@link Group}. Assigns proper {@link Group#id() ids} to created groups.
 */
public class Groups implements ReadableGroups {
    private final Map<String, Group> byName = new HashMap<>();
    private final List<Group> byId = new ArrayList<>();
    private int nextId = 0;

    public Groups() {}

    /**
     * {@link #getOrCreate(String, String)} w/o specific {@link IdType}.
     */
    public Group getOrCreate(String name) {
        return getOrCreate(name, null);
    }

    /**
     * @param name group name or {@code null} for the "global" group.
     * @param specificIdType optional type that IDs in this group should be parsed as,
     * otherwise if {@code null} the globally defined {@link IdType} is used.
     * @return {@link Group} for the given name. If the group doesn't already exist it will be created
     * with a new id. If {@code name} is {@code null} then the "global" group is returned.
     * This method also prevents mixing global and non-global groups, i.e. if first call is {@code null},
     * then consecutive calls have to specify {@code null} name as well. The same holds true for non-null values.
     */
    public synchronized Group getOrCreate(String name, String specificIdType) {
        Group group = byName.get(name);
        if (group == null) {
            byName.put(name, group = new Group(nextId++, name, specificIdType));
            byId.add(group);
        } else {
            checkState(
                    Objects.equals(specificIdType, group.specificIdType()),
                    "Group '%s' has different specific type %s in different places. Was created with '%s' and later used with '%s'",
                    group.name(),
                    group.specificIdType(),
                    specificIdType);
        }
        return group;
    }

    @Override
    public synchronized Group get(String name) {
        Group group = byName.get(name);
        if (group == null) {
            throw new HeaderException("Group '" + name + "' not found. Available groups are: " + groupNames());
        }
        return group;
    }

    @Override
    public Group get(int id) {
        if (id < 0 || id >= byId.size()) {
            throw new HeaderException("Group with id " + id + " not found");
        }
        return byId.get(id);
    }

    private String groupNames() {
        return Arrays.toString(byName.keySet().toArray(new String[0]));
    }

    @Override
    public int size() {
        return nextId;
    }
}
