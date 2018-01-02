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
package org.neo4j.kernel.impl.api.index.inmemory;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static java.util.Objects.requireNonNull;

@Service.Implementation(KernelExtensionFactory.class)
public class InMemoryIndexProviderFactory extends KernelExtensionFactory<InMemoryIndexProviderFactory.Dependencies>
{
    public static final String KEY = "in-memory-index";

    public static final SchemaIndexProvider.Descriptor PROVIDER_DESCRIPTOR =
            new SchemaIndexProvider.Descriptor( KEY, "1.0" );

    private final InMemoryIndexProvider singleProvider;

    public interface Dependencies
    {
    }

    public InMemoryIndexProviderFactory()
    {
        super( KEY );
        this.singleProvider = null;
    }

    public InMemoryIndexProviderFactory( InMemoryIndexProvider singleProvider )
    {
        super( KEY );
        this.singleProvider = requireNonNull( singleProvider, "provider" );
    }

    @Override
    public Lifecycle newKernelExtension( Dependencies dependencies ) throws Throwable
    {
        return singleProvider != null ? singleProvider : new InMemoryIndexProvider();
    }
}
