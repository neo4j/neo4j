/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.graphdb.factory;

import java.util.Collections;
import java.util.function.Predicate;

import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.ha.ClusterManager;

public class TestHighlyAvailableGraphDatabaseFactory extends HighlyAvailableGraphDatabaseFactory
{
    @Override
    protected void configure( GraphDatabaseBuilder builder )
    {
        super.configure( builder );
        builder.setConfig( ClusterManager.CONFIG_FOR_SINGLE_JVM_CLUSTER );
        builder.setConfig( GraphDatabaseSettings.store_internal_log_level, "DEBUG" );
    }

    public TestHighlyAvailableGraphDatabaseFactory addKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        getCurrentState().addKernelExtensions( newKernelExtensions );
        return this;
    }

    public TestHighlyAvailableGraphDatabaseFactory addKernelExtension( KernelExtensionFactory<?> newKernelExtension )
    {
        return addKernelExtensions( Collections.singletonList( newKernelExtension ) );
    }

    public TestHighlyAvailableGraphDatabaseFactory setKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        getCurrentState().setKernelExtensions( newKernelExtensions );
        return this;
    }

    public TestHighlyAvailableGraphDatabaseFactory removeKernelExtensions( Predicate<KernelExtensionFactory<?>> toRemove )
    {
        getCurrentState().removeKernelExtensions( toRemove );
        return this;
    }
}
