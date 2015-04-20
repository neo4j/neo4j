/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
    private final int label;
    private final Kind kind;
    private final long id;

    public AbstractSchemaRule( long id, int label, Kind kind )
    {
        this.id = id;
        this.label = label;
        this.kind = kind;
    }

    @Override
    public long getId()
    {
        return this.id;
    }

    @Override
    public final int getLabel()
    {
        return this.label;
    }

    @Override
    public final Kind getKind()
    {
        return this.kind;
    }

    @Override
    public int length()
    {
        return 4 /*label id*/ + 1 /*kind id*/;
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        target.putInt( label );
        target.put( kind.id() );
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = prime + kind.hashCode();
        return prime * result + label;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj == null )
        {
            return false;
        }
        if ( getClass() != obj.getClass() )
        {
            return false;
        }
        AbstractSchemaRule other = (AbstractSchemaRule) obj;
        if ( kind != other.kind )
        {
            return false;
        }
        if ( label != other.label )
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[id="+ id +", label="+label+", kind="+ kind + innerToString() + "]";
    }

    protected abstract String innerToString();
}
