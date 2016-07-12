/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.store.record;

import java.nio.ByteBuffer;

public abstract class AbstractSchemaRule implements SchemaRule
{
    protected final Kind kind;
    protected final long id;

    public AbstractSchemaRule( long id, Kind kind )
    {
        this.id = id;
        this.kind = kind;
    }

    @Override
    public long getId()
    {
        return this.id;
    }

    @Override
    public final Kind getKind()
    {
        return this.kind;
    }

    @Override
    public abstract int length();

    @Override
    public abstract void serialize( ByteBuffer target );

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        AbstractSchemaRule that = (AbstractSchemaRule) o;
        return kind == that.kind;
    }

    @Override
    public int hashCode()
    {
        return kind.hashCode();
    }

    @Override
    public abstract String toString();
}
