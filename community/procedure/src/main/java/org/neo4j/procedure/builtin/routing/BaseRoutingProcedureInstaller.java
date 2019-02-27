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
package org.neo4j.procedure.builtin.routing;

import org.eclipse.collections.impl.factory.Lists;

import java.util.List;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

public abstract class BaseRoutingProcedureInstaller
{
    public static final List<String> DEFAULT_NAMESPACE = Lists.immutable.of( "dbms", "routing" ).castToList();
    private static final List<String> LEGACY_NAMESPACE = Lists.immutable.of( "dbms", "cluster", "routing" ).castToList();

    public final void install( GlobalProcedures globalProcedures ) throws ProcedureException
    {
        // make procedure available as with both `dbms.routing` and old `dbms.cluster.routing` namespaces
        globalProcedures.register( createProcedure( DEFAULT_NAMESPACE ) );
        globalProcedures.register( createProcedure( LEGACY_NAMESPACE ) );
    }

    protected abstract CallableProcedure createProcedure( List<String> namespace );
}
