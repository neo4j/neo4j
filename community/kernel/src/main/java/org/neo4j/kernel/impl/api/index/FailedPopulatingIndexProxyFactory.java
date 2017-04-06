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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;

public class FailedPopulatingIndexProxyFactory implements FailedIndexProxyFactory
{
    private final IndexDescriptor descriptor;
    private final SchemaIndexProvider.Descriptor providerDescriptor;
    private final IndexPopulator populator;
    private final String indexUserDescription;
    private final IndexCountsRemover indexCountsRemover;
    private final LogProvider logProvider;

    FailedPopulatingIndexProxyFactory( IndexDescriptor descriptor,
                                       SchemaIndexProvider.Descriptor providerDescriptor,
                                       IndexPopulator populator,
                                       String indexUserDescription,
                                       IndexCountsRemover indexCountsRemover,
                                       LogProvider logProvider )
    {
        this.descriptor = descriptor;
        this.providerDescriptor = providerDescriptor;
        this.populator = populator;
        this.indexUserDescription = indexUserDescription;
        this.indexCountsRemover = indexCountsRemover;
        this.logProvider = logProvider;
    }

    @Override
    public IndexProxy create( Throwable failure )
    {
        return
            new FailedIndexProxy(
                descriptor, providerDescriptor,
                indexUserDescription, populator, failure( failure ), indexCountsRemover, logProvider );
    }
}
