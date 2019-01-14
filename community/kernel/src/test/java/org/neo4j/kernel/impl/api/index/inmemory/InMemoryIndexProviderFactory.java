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
package org.neo4j.kernel.impl.api.index.inmemory;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static java.util.Objects.requireNonNull;

@Service.Implementation( KernelExtensionFactory.class )
public class InMemoryIndexProviderFactory extends KernelExtensionFactory<InMemoryIndexProviderFactory.Dependencies>
{
    public static final String KEY = "in-memory-index";
    public static final String VERSION = "1.0";

    public static final IndexProvider.Descriptor PROVIDER_DESCRIPTOR =
            new IndexProvider.Descriptor( KEY, VERSION );

    private final IndexProvider singleProvider;

    public interface Dependencies
    {
    }

    public InMemoryIndexProviderFactory()
    {
        super( KEY );
        this.singleProvider = null;
    }

    public InMemoryIndexProviderFactory( IndexProvider singleProvider )
    {
        super( KEY );
        this.singleProvider = requireNonNull( singleProvider, "provider" );
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies )
    {
        return singleProvider != null ? singleProvider : new InMemoryIndexProvider();
    }
}
