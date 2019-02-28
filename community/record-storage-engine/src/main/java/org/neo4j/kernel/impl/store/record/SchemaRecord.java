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
package org.neo4j.kernel.impl.store.record;

public class SchemaRecord extends PrimitiveRecord
{
    public static final byte COMMAND_HAS_NO_SCHEMA_RULE = 0;
    public static final byte COMMAND_HAS_SCHEMA_RULE = 1;
    public static final byte SCHEMA_FLAG_IS_CONSTRAINT = 1;

    private boolean constraint;

    public SchemaRecord( long id )
    {
        super( id );
    }

    @Override
    public void setIdTo( PropertyRecord property )
    {
        property.setSchemaRuleId( getId() );
    }

    @Override
    public SchemaRecord initialize( boolean inUse, long nextProp )
    {
        super.initialize( inUse, nextProp );
        return this;
    }

    @Override
    public SchemaRecord clone() throws CloneNotSupportedException
    {
        return (SchemaRecord) super.clone();
    }

    @Override
    public String toString()
    {
        return "SchemaRecord[" + getId() + ",used=" + inUse() + ",nextProp=" + nextProp + ",constraint=" + constraint + "]";
    }

    public boolean isConstraint()
    {
        return constraint;
    }

    public void setConstraint( boolean constraint )
    {
        this.constraint = constraint;
    }
}
