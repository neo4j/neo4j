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
