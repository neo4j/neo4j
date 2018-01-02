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
package org.neo4j.kernel.builtinprocs;

import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.QualifiedName;

import static java.util.Collections.singletonList;
import static org.neo4j.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTList;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTString;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

/**
 * This procedure lists "components" and their version.
 * While components are currently hard-coded, it is intended
 * that this implementation will be replaced once a clean
 * system for component assembly exists where we could dynamically
 * get a list of which components are loaded and what versions of them.
 *
 * This way, it works as a general mechanism into which capabilities
 * a given Neo4j system has, and which version of those components
 * are in use.
 *
 * This would include things like Kernel, Storage Engine, Query Engines,
 * Bolt protocol versions et cetera.
 */
public class ListComponentsProcedure extends CallableProcedure.BasicProcedure
{
    private final String neo4jVersion;
    private final String neo4jEdition;

    public ListComponentsProcedure( QualifiedName name, String neo4jVersion, String neo4jEdition )
    {
        super( procedureSignature( name )
                .out( "name", NTString )
                // Since Bolt, Cypher and other components support multiple versions
                // at the same time, list of versions rather than single version.
                .out( "versions", NTList( NTString ) )
                .out( "edition", NTString )
                .description( "List DBMS components and their versions." )
                .build() );
        this.neo4jVersion = neo4jVersion;
        this.neo4jEdition = neo4jEdition;
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply( Context ctx, Object[] input )
            throws ProcedureException
    {
        return asRawIterator( singletonList(
                new Object[]{"Neo4j Kernel", singletonList( neo4jVersion ), neo4jEdition}).iterator() );
    }
}
