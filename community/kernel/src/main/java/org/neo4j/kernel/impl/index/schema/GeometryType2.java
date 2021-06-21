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
package org.neo4j.kernel.impl.index.schema;

import java.util.StringJoiner;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

class GeometryType2 extends Type
{
    //TODO not implemented yet
    GeometryType2( byte typeId )
    {
        super( ValueGroup.GEOMETRY, typeId, PointValue.MIN_VALUE, PointValue.MAX_VALUE );
    }

    @Override
    int valueSize( GenericKey<?> state )
    {
        return 0;
    }

    @Override
    void copyValue( GenericKey<?> to, GenericKey<?> from )
    {
    }

    @Override
    Value asValue( GenericKey<?> state )
    {
        return null;
    }

    @Override
    int compareValue( GenericKey<?> left, GenericKey<?> right )
    {
        return 0;
    }

    @Override
    void putValue( PageCursor cursor, GenericKey<?> state )
    {
    }

    @Override
    boolean readValue( PageCursor cursor, int size, GenericKey<?> into )
    {
        return false;
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey<?> state )
    {

    }
}
