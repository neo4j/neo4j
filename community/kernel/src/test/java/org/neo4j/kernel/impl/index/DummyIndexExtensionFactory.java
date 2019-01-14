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
package org.neo4j.kernel.impl.index;

import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.spi.explicitindex.IndexProviders;

public class DummyIndexExtensionFactory extends KernelExtensionFactory<DummyIndexExtensionFactory.Dependencies>
{
    public static final String IDENTIFIER = "test-dummy-neo-index";
    static final String KEY_FAIL_ON_MUTATE = "fail_on_mutate";

    public DummyIndexExtensionFactory()
    {
        super( IDENTIFIER );
    }

    public interface Dependencies
    {
        IndexProviders getIndexProviders();
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies )
    {
        IndexProviders indexProviders = dependencies.getIndexProviders();
        return new Extension( indexProviders );
    }

    private static class Extension extends LifecycleAdapter
    {
        private final IndexProviders indexProviders;

        Extension( IndexProviders indexProviders )
        {
            this.indexProviders = indexProviders;
        }

        @Override
        public void init()
        {
            indexProviders.registerIndexProvider( IDENTIFIER, new DummyIndexImplementation() );
        }

        @Override
        public void shutdown()
        {
            indexProviders.unregisterIndexProvider( IDENTIFIER );
        }
    }
}
