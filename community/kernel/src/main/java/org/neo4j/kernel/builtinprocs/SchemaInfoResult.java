/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.builtinprocs;

import java.util.List;

public class SchemaInfoResult
{
    /**
     * Indicates whether the entry is a node or a relationship
     */
    public final String type;

    /**
     * A combination of labels or a relationship
     */
    public final List<String> nodeLabelsOrRelType;

    /**
     * A property that occurs on the given label combination / relationship type or null
     */
    public final String property;

    /**
     * A List containing all CypherTypes of the given property on the given label combination / relationship type or null
     */
    public final List<String> cypherTypes;

    /**
     * Indicates whether the property is present on all similar nodes / relationships (= false) or not (= true)
     */
    public final boolean nullable;

    public SchemaInfoResult( String type, List<String> nodeLabelsOrRelType, String property, List<String> cypherTypes, boolean nullable )
    {
        this.type = type;
        this.nodeLabelsOrRelType = nodeLabelsOrRelType;
        this.property = property;
        this.cypherTypes = cypherTypes;
        this.nullable = nullable;
    }
}
