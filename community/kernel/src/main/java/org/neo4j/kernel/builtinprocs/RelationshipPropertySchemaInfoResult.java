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

@SuppressWarnings( "WeakerAccess" )
public class RelationshipPropertySchemaInfoResult
{
    /**
     * A relationship type
     */
    public final TextValue relType;

    /**
     * A property name that occurs on the given relationship type or null
     */
    public final AnyValue propertyName;

    /**
     * A List containing all types of the given property on the given relationship type or null
     */
    public final AnyValue propertyTypes;

    /**
     * Indicates whether the property is present on all similar relationships (= true) or not (= false)
     */
    public final BooleanValue mandatory;

    public RelationshipPropertySchemaInfoResult( TextValue relType, AnyValue propertyName, AnyValue cypherTypes,
            BooleanValue mandatory )
    {
        this.relType = relType;
        this.propertyName = propertyName;
        this.propertyTypes = cypherTypes;
        this.mandatory  = mandatory;
    }
}
