/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;

@SuppressWarnings( "WeakerAccess" )
public class NodePropertySchemaInfoResult
{
    /**
     * A combination of escaped label names interleaved by ":"
     */
    public final TextValue nodeType;

    /**
     * A list of label names
     */
    public final ListValue nodeLabels;
    /**
     * A property name that occurs on the given label combination or null
     */
    public final AnyValue propertyName;

    /**
     * A List containing all types of the given property on the given label combination or null
     */
    public final AnyValue propertyTypes;

    /**
     * Indicates whether the property is present on all similar nodes (= true) or not (= false)
     */
    public final BooleanValue mandatory;

    public NodePropertySchemaInfoResult( TextValue nodeType, ListValue nodeLabelsList, AnyValue propertyName, AnyValue cypherTypes, BooleanValue mandatory )
    {
        this.nodeType = nodeType;
        this.nodeLabels = nodeLabelsList;
        this.propertyName = propertyName;
        this.propertyTypes = cypherTypes;
        this.mandatory  = mandatory;
    }
}
