/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;

public abstract class IndexPopulationFailure
{
    public abstract String asString();
    
    public abstract IndexPopulationFailedKernelException asIndexPopulationFailure(
            IndexDescriptor descriptor, String indexUserDescriptor );

    public static IndexPopulationFailure failure( final Throwable failure )
    {
        return new IndexPopulationFailure()
        {
            @Override
            public String asString()
            {
                return Exceptions.stringify( failure );
            }

            @Override
            public IndexPopulationFailedKernelException asIndexPopulationFailure(
                    IndexDescriptor descriptor, String indexUserDescription )
            {
                return new IndexPopulationFailedKernelException( descriptor, indexUserDescription, failure );
            }
        };
    }

    public static IndexPopulationFailure failure( final String failure )
    {
        return new IndexPopulationFailure()
        {
            @Override
            public String asString()
            {
                return failure;
            }

            @Override
            public IndexPopulationFailedKernelException asIndexPopulationFailure(
                    IndexDescriptor descriptor, String indexUserDescription )
            {
                return new IndexPopulationFailedKernelException( descriptor, indexUserDescription, failure );
            }
        };
    }
}
