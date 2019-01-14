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
package org.neo4j.kernel.impl.index;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/*
 This seems like a weird test, but it's necessary because we have a binding between the Enum name
 and the name on the filesystem. On case-sensitive file systems, we need a consistent lower-cased name
 for the entity type.
 */
public class IndexEntityTypeTest
{
    @Test
    public void shouldLowerCaseEnumName()
    {
        assertEquals( "node", IndexEntityType.Node.nameToLowerCase() );
        assertEquals( "relationship", IndexEntityType.Relationship.nameToLowerCase() );
    }
}
