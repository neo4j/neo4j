/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.harness.internal;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

class HarnessRegisteredProcs
{
    private final List<Class<?>> procs = new ArrayList<>();
    private final List<Class<?>> functions = new ArrayList<>();
    private final List<Class<?>> aggregationFunctions = new ArrayList<>();

    void addProcedure( Class<?> procedureClass )
    {
        this.procs.add( procedureClass );
    }

    void addFunction( Class<?> functionClass )
    {
        this.functions.add( functionClass );
    }

    void addAggregationFunction( Class<?> functionClass )
    {
        this.aggregationFunctions.add( functionClass );
    }

    @SuppressWarnings( "deprecation" )
    void applyTo( GlobalProcedures globalProcedures ) throws KernelException
    {
        for ( Class<?> cls : procs )
        {
            globalProcedures.registerProcedure( cls );
        }

        for ( Class<?> cls : functions )
        {
            globalProcedures.registerFunction( cls );
        }

        for ( Class<?> cls : aggregationFunctions )
        {
            globalProcedures.registerAggregationFunction( cls );
        }
    }
}
