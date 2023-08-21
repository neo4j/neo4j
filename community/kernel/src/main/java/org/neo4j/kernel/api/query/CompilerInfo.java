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
package org.neo4j.kernel.api.query;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class CompilerInfo {
    private final String planner;
    private final String runtime;
    private final List<SchemaIndexUsage> indexes;
    private final List<RelationshipTypeIndexUsage> relationshipTypeIndexes;
    private final List<LookupIndexUsage> lookupIndexes;

    public CompilerInfo(
            String planner,
            String runtime,
            List<SchemaIndexUsage> indexes,
            List<RelationshipTypeIndexUsage> relationshipTypeIndexes,
            List<LookupIndexUsage> lookupIndexes) {
        this.planner = planner;
        this.runtime = runtime;
        this.indexes = indexes;
        this.relationshipTypeIndexes = relationshipTypeIndexes;
        this.lookupIndexes = lookupIndexes;
    }

    public CompilerInfo(String planner, String runtime, List<SchemaIndexUsage> indexes) {
        this(planner, runtime, indexes, Collections.emptyList(), Collections.emptyList());
    }

    public String planner() {
        return planner.toLowerCase(Locale.ROOT);
    }

    public String runtime() {
        return runtime.toLowerCase(Locale.ROOT);
    }

    public List<SchemaIndexUsage> indexes() {
        return indexes;
    }

    public List<RelationshipTypeIndexUsage> relationshipTypeIndexes() {
        return relationshipTypeIndexes;
    }

    public List<LookupIndexUsage> lookupIndexes() {
        return lookupIndexes;
    }
}
