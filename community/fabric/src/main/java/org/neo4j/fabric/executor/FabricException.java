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
package org.neo4j.fabric.executor;

import java.util.OptionalLong;

import org.neo4j.kernel.api.exceptions.HasQuery;
import org.neo4j.kernel.api.exceptions.Status;

public class FabricException extends RuntimeException implements Status.HasStatus, HasQuery
{
    private final Status statusCode;
    private final OptionalLong queryId;

    public FabricException( Status statusCode, Throwable cause )
    {
        super( cause );
        this.statusCode = statusCode;
        this.queryId = OptionalLong.empty();
    }

    public FabricException( Status statusCode, String message, Object... parameters )
    {
        super( String.format( message, parameters ) );
        this.statusCode = statusCode;
        this.queryId = OptionalLong.empty();
    }

    public FabricException( Status statusCode, String message, Throwable cause )
    {
        super( message, cause );
        this.statusCode = statusCode;
        this.queryId = OptionalLong.empty();
    }

    public FabricException( Status statusCode, String message, Throwable cause, OptionalLong queryId )
    {
        super( message, cause );
        this.statusCode = statusCode;
        this.queryId = queryId;
    }

    @Override
    public Status status()
    {
        return statusCode;
    }

    @Override
    public OptionalLong query()
    {
        return queryId;
    }
}
