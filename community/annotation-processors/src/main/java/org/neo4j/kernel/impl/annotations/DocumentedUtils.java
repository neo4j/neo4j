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
package org.neo4j.kernel.impl.annotations;

import java.lang.reflect.Method;

/**
 * Utility methods for {@link Documented} annotation.
 */
public final class DocumentedUtils
{
    private DocumentedUtils()
    {
        throw new RuntimeException( "Should not be instantiated." );
    }

    public static String extractMessage( Method method )
    {
        String message;
        Documented annotation = method.getAnnotation( Documented.class );
        if ( annotation != null && !"".equals( annotation.value() ) )
        {
            message = annotation.value();
        }
        else
        {
            message = method.getName();
        }
        return message;
    }

    public static String extractFormattedMessage( Method method, Object[] args )
    {
        return String.format( extractMessage( method ), args );
    }
}
