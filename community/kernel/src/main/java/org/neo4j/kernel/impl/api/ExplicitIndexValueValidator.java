/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.impl.util.Validator;

public class ExplicitIndexValueValidator implements Validator<Object>
{
    public static ExplicitIndexValueValidator INSTANCE = new ExplicitIndexValueValidator();

    private ExplicitIndexValueValidator()
    {
    }

    @Override
    public void validate( Object value )
    {
        if ( value == null )
        {
            throw new IllegalArgumentException( "Null value" );
        }
        if ( value instanceof String )
        {
            IndexValueLengthValidator.INSTANCE.validate( ((String)value).getBytes() );
        }
        if ( !(value instanceof Number) && value.toString() == null )
        {
            throw new IllegalArgumentException( "Value of type " + value.getClass() + " has null toString" );
        }
    }
}
