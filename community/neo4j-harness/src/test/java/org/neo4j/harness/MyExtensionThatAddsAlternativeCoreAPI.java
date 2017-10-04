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
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

// Similar to the MyExtensionThatAddsInjectable, this demonstrates a
// non-public mechanism for adding new context components, but in this
// case the goal is to provide alternative Core API's and as such it wraps
// the old Core API.
public class MyExtensionThatAddsAlternativeCoreAPI
        extends KernelExtensionFactory<MyExtensionThatAddsAlternativeCoreAPI.Dependencies>
{
    public MyExtensionThatAddsAlternativeCoreAPI()
    {
        super( "my-ext" );
    }

    @Override
    public Lifecycle newInstance( KernelContext context,
            Dependencies dependencies ) throws Throwable
    {
        dependencies.procedures().registerComponent( MyCoreAPI.class,
                ctx -> new MyCoreAPI( dependencies.getGraphDatabaseAPI(), dependencies.txBridge(),
                        dependencies.logService().getUserLog( MyCoreAPI.class ) ), true );
        return new LifecycleAdapter();
    }

    public interface Dependencies
    {
        LogService logService();

        Procedures procedures();

        GraphDatabaseAPI getGraphDatabaseAPI();

        ThreadToStatementContextBridge txBridge();

    }
}
