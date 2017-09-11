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
package org.neo4j.harness;

import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

// This ensures a non-public mechanism for adding new context components
// to Procedures is testable via Harness. While this is not public API,
// this is a vital mechanism to cover use cases Procedures need to cover,
// and is in place as an approach that should either eventually be made
// public, or the relevant use cases addressed in other ways.
public class MyExtensionThatAddsInjectable
        extends KernelExtensionFactory<MyExtensionThatAddsInjectable.Dependencies>
{
    public MyExtensionThatAddsInjectable()
    {
        super( "my-ext" );
    }

    @Override
    public Lifecycle newInstance( KernelContext context,
            Dependencies dependencies ) throws Throwable
    {
        dependencies.procedures().registerComponent( SomeService.class, ctx -> new SomeService(), true );
        return new LifecycleAdapter();
    }

    public interface Dependencies
    {
        Procedures procedures();
    }
}
