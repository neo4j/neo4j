/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature.ProcedureName;

import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Iterators.map;
import static org.neo4j.kernel.api.proc.CallableProcedure.Context.KERNEL_TRANSACTION;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class ListIndexesProcedure extends CallableProcedure.BasicProcedure
{

    //When we have decided on what to call different indexes
    //this should probably be moved to some more central place
    public enum IndexType
    {
        NODE_LABEL_PROPERTY( "node_label_property" ),
        NODE_UNIQUE_PROPERTY( "node_unique_property" );

        private final String typeName;

        IndexType( String typeName )
        {
            this.typeName = typeName;
        }

        public String typeName()
        {
            return typeName;
        }
    }

    protected ListIndexesProcedure(ProcedureName procedureName)
    {
        super( procedureSignature( procedureName )
                .out( "description", Neo4jTypes.NTString )
                .out( "state", Neo4jTypes.NTString )
                .out( "type", Neo4jTypes.NTString )
                .build() );
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply( Context ctx, Object[] input )
            throws ProcedureException
    {
        Statement statement = ctx.get( KERNEL_TRANSACTION ).acquireStatement();
        TokenNameLookup tokens = new StatementTokenNameLookup(statement.readOperations());

        List<IndexDescriptor> indexes =
                asList( statement.readOperations().indexesGetAll() );

        Set<IndexDescriptor> uniqueIndexes = asSet( statement.readOperations().uniqueIndexesGetAll() );
        indexes.addAll( uniqueIndexes );
        indexes.sort( (a,b) -> a.userDescription(tokens).compareTo( b.userDescription(tokens) ) );

        return format( indexes, statement, tokens, (descriptor) -> {
            if (uniqueIndexes.contains( descriptor )) return IndexType.NODE_UNIQUE_PROPERTY;
            else return IndexType.NODE_LABEL_PROPERTY;
        } );
    }

    private RawIterator<Object[],ProcedureException> format(List<IndexDescriptor> indexes,
            Statement statement, TokenNameLookup tokens, Function<IndexDescriptor, IndexType> type)
    {
        return map( ( index ) -> {
                    try
                    {
                        return new Object[]{"INDEX ON " + index.userDescription( tokens ),
                                statement.readOperations().indexGetState( index ).toString().toLowerCase(),
                                type.apply( index ).typeName()};
                    }
                    catch ( IndexNotFoundKernelException e )
                    {
                        throw new ProcedureException( Status.Schema.IndexNotFound, e,
                                "No index on ", index.userDescription( tokens ) );
                    }
                },
                asRawIterator( indexes.iterator() ) );
    }
}
