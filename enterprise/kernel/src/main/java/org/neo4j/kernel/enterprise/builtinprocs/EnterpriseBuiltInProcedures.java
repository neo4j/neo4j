/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.enterprise.builtinprocs;

import java.util.stream.Stream;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.builtinprocs.BuiltInProcedures;
import org.neo4j.kernel.builtinprocs.IndexProcedures;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Mode.SCHEMA;

@SuppressWarnings( "unused" )
public class EnterpriseBuiltInProcedures
{
    @Context
    public KernelTransaction tx;

    @Context
    public DependencyResolver resolver;

    @Description( "Create a unique property constraint with index backed by specified index provider " +
            "(for example: CALL db.createUniquePropertyConstraint(\":Person(name)\", \"lucene+native-2.0\")) - " +
            "YIELD index, providerName, status" )
    @Procedure( name = "db.createUniquePropertyConstraint", mode = SCHEMA )
    public Stream<BuiltInProcedures.SchemaIndexInfo> createUniquePropertyConstraint(
            @Name( "index" ) String index,
            @Name( "providerName" ) String providerName )
            throws ProcedureException
    {
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            return indexProcedures.createUniquePropertyConstraint( index, providerName );
        }
    }

    @Description( "Create a node key constraint with index backed by specified index provider " +
            "(for example: CALL db.createNodeKey(\":Person(name)\", \"lucene+native-2.0\")) - " +
            "YIELD index, providerName, status" )
    @Procedure( name = "db.createNodeKey", mode = SCHEMA )
    public Stream<BuiltInProcedures.SchemaIndexInfo> createNodeKey(
            @Name( "index" ) String index,
            @Name( "providerName" ) String providerName )
            throws ProcedureException
    {
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            return indexProcedures.createNodeKey( index, providerName );
        }
    }

    private IndexProcedures indexProcedures()
    {
        return new IndexProcedures( tx, resolver.resolveDependency( IndexingService.class ) );
    }
}
