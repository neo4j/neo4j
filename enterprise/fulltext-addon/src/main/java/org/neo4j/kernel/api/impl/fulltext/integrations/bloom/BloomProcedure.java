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
package org.neo4j.kernel.api.impl.fulltext.integrations.bloom;

import java.util.ArrayList;
import java.util.Arrays;

import org.neo4j.collection.RawIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.impl.fulltext.ReadOnlyFulltext;
import org.neo4j.kernel.api.impl.fulltext.LuceneFulltext;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.QualifiedName;

import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class BloomProcedure extends CallableProcedure.BasicProcedure
{
    public static final String OUTPUT_NAME = "entityid";
    private static final String PROCEDURE_NAME = "bloomFulltext";
    private static final String[] PROCEDURE_NAMESPACE = {"db", "fulltext"};
    private final LuceneFulltext luceneFulltext;
    private String type;

    BloomProcedure( String type, LuceneFulltext luceneFulltext )
    {
        super( procedureSignature( new QualifiedName( PROCEDURE_NAMESPACE, PROCEDURE_NAME + type ) ).in( "terms",
                Neo4jTypes.NTList( Neo4jTypes.NTString ) ).out( OUTPUT_NAME, Neo4jTypes.NTInteger ).description(
                String.format( "Queries the bloom index for %s.", type ) ).build() );
        this.luceneFulltext = luceneFulltext;
        this.type = type;
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        String[] query = ((ArrayList<String>) input[0]).toArray( new String[0] );
        try ( ReadOnlyFulltext indexReader = luceneFulltext.getIndexReader() )
        {
            PrimitiveLongIterator primitiveLongIterator = indexReader.fuzzyQuery( query );
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
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, e, "Failed to query bloom index for " + type, input );
        }
    }
}
