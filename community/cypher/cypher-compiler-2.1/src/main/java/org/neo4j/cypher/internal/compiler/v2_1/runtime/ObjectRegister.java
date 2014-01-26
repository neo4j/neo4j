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

public final class ObjectRegister implements Register
{
    private final Registers registers;
    private final int idx;

    public ObjectRegister( Registers registers, int idx )
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
        return registers.getObjectRegister( idx );
    }

    @Override
    public void setObject( Object value )
    {
        registers.setObjectRegister( idx, value );
    }

    @Override
    public void copyFrom( Registers registers )
    {
        setObject( registers.getObjectRegister( idx ) );
    }

    @Override
    public void copyTo( Registers registers )
    {
        registers.setObjectRegister( idx, getObject() );
    }
}
