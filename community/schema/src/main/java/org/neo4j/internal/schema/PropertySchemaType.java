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
package org.neo4j.internal.schema;

/**
 * This enum signifies how this schema should behave in regards to updates.
 * {@link PropertySchemaType#COMPLETE_ALL_TOKENS} signifies that this schema unit only should be affected by updates that match the entire schema,
 * i.e. when all properties are present. If you are unsure then this is probably what you want.
 * {@link PropertySchemaType#PARTIAL_ANY_TOKEN} signifies that this schema unit should be affected by any update that is partial match of the schema,
 *  i.e. at least one of the properties of this schema unit is present.
 */
public enum PropertySchemaType
{
    COMPLETE_ALL_TOKENS,
    PARTIAL_ANY_TOKEN
}
