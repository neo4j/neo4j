/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.helper;

import java.util.function.Predicate;

import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;

public class IsStoreClosed implements Predicate<Throwable>
{
    @Override
    public boolean test( Throwable ex )
    {

        if ( ex == null )
        {
            return false;
        }

        if ( ex instanceof UnsatisfiedDependencyException )
        {
            return true;
        }

        if ( ex instanceof IllegalStateException )
        {
            String message = ex.getMessage();
            return message.startsWith( "MetaDataStore for file " ) && message.endsWith( " is closed" );
        }

        return test( ex.getCause() );
    }
}
