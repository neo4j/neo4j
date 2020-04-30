/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.fabric.executor;

import org.neo4j.kernel.api.exceptions.Status;

public class Exceptions
{
    public static RuntimeException transform( Status defaultStatus, Throwable t )
    {
        var unwrapped = reactor.core.Exceptions.unwrap( t );
        String message = unwrapped.getMessage();

        // preserve the original exception if possible
        // or try to preserve  at least the original status
        if ( unwrapped instanceof Status.HasStatus )
        {
            if ( unwrapped instanceof RuntimeException )
            {
                return (RuntimeException) unwrapped;
            }

            return new FabricException( ((Status.HasStatus) unwrapped).status(), message, unwrapped );
        }

        return new FabricException( defaultStatus, message, unwrapped );
    }
}
