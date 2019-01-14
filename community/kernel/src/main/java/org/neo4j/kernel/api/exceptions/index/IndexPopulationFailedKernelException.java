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
package org.neo4j.kernel.api.exceptions.index;

import java.util.Arrays;

import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.Status;

public class IndexPopulationFailedKernelException extends KernelException
{
    private static final String FORMAT_MESSAGE = "Failed to populate index for %s [labelId: %d, properties %s]";

    public IndexPopulationFailedKernelException( SchemaDescriptor descriptor, String indexUserDescription,
            Throwable cause )
    {
        super( Status.Schema.IndexCreationFailed, cause, FORMAT_MESSAGE, indexUserDescription,
                descriptor.keyId(), Arrays.toString( descriptor.getPropertyIds() ) );
    }

    public IndexPopulationFailedKernelException( SchemaDescriptor descriptor, String indexUserDescription,
            String message )
    {
        super( Status.Schema.IndexCreationFailed, FORMAT_MESSAGE + ", due to " + message,
                indexUserDescription, descriptor.keyId(), Arrays.toString( descriptor.getPropertyIds() ) );
    }
}
