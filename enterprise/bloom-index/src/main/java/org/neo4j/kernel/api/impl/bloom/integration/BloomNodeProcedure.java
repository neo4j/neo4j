/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.bloom.integration;

import java.util.Arrays;

import org.neo4j.collection.RawIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.impl.bloom.BloomIndex;
import org.neo4j.kernel.api.impl.bloom.BloomIndexReader;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.QualifiedName;

import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class BloomNodeProcedure extends CallableProcedure.BasicProcedure
{
    private static final String PROCEDURE_NAME = "bloomNodes";
    private static final String[] PROCEDURE_NAMESPACE = {"dbms", "bloom"};
    private static final String OUTPUT_NAME = "nodeid";
    private BloomIndex bloomIndex;

    BloomNodeProcedure( BloomIndex bloomIndex )
    {
        super( procedureSignature( new QualifiedName( PROCEDURE_NAMESPACE, PROCEDURE_NAME ) ).in( "terms", Neo4jTypes.NTList( Neo4jTypes.NTString ) ).out(
                OUTPUT_NAME, Neo4jTypes.NTInteger ).description( "Queries the bloom index for nodes." ).build() );
        this.bloomIndex = bloomIndex;
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        String[] query = Arrays.stream( input ).map( Object::toString ).toArray( String[]::new );
        try ( BloomIndexReader nodeReader = bloomIndex.getNodeReader() )
        {
            PrimitiveLongIterator primitiveLongIterator = nodeReader.query( query );
            return new RawIterator<Object[],ProcedureException>()
            {
                @Override
                public boolean hasNext() throws ProcedureException
                {
                    return primitiveLongIterator.hasNext();
                }

                @Override
                public Object[] next() throws ProcedureException
                {
                    return new Long[]{primitiveLongIterator.next()};
                }
            };
        }
        catch ( Exception e )
        {
            throw new ProcedureException( Status.statusCodeOf( e ), e, "Failed to query bloom index for nodes", input );
        }
    }
}
