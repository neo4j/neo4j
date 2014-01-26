/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.runtime;

public final class EntityRegister implements Register
{
    private final Registers registers;
    private final int idx;

    public EntityRegister( Registers registers, int idx )
    {
        this.registers = registers;
        this.idx = idx;
    }

    @Override
    public Registers registers()
    {
        return registers;
    }

    @Override
    public Object getObject()
    {
        return (Long) getEntity();
    }

    @Override
    public void setObject( Object value )
    {
        setEntity( (Long) value );
    }

    public long getEntity()
    {
        return registers.getEntityRegister( idx );
    }

    public void setEntity( long value )
    {
        registers.setEntityRegister( idx, value );
    }

    @Override
    public void copyFrom( Registers registers )
    {
        setEntity( registers.getEntityRegister( idx ) );
    }

    @Override
    public void copyTo( Registers registers )
    {
        registers.setEntityRegister( idx, getEntity() );
    }
}
