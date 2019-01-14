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
package org.neo4j.internal.collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;

/**
 * The Graph Counts section holds all data that is available form the counts store, plus metadata
 * about the available indexes and constraints. This essentially captures all the knowledge the
 * planner has when planning, meaning that the data from this section could be used to investigate
 * planning problems.
 */
final class GraphCountsSection
{
    private GraphCountsSection()
    { // only static functionality
    }

    static Stream<RetrieveResult> retrieve( Kernel kernel, Anonymizer anonymizer )
            throws TransactionFailureException, IndexNotFoundKernelException
    {
        try ( Transaction tx = kernel.beginTransaction( Transaction.Type.explicit, LoginContext.AUTH_DISABLED ) )
        {
            TokenRead tokens = tx.tokenRead();
            Read read = tx.dataRead();

            Map<String,Object> data = new HashMap<>();
            data.put( "nodes", nodeCounts( tokens, read, anonymizer ) );
            data.put( "relationships", relationshipCounts( tokens, read, anonymizer ) );
            data.put( "indexes", indexes( tokens, tx.schemaRead(), anonymizer ) );
            data.put( "constraints", constraints( tokens, tx.schemaRead(), anonymizer ) );

            return Stream.of( new RetrieveResult( Sections.GRAPH_COUNTS, data ) );
        }
    }

    private static List<Map<String,Object>> nodeCounts( TokenRead tokens, Read read, Anonymizer anonymizer )
    {
        List<Map<String,Object>> nodeCounts = new ArrayList<>();
        Map<String,Object> nodeCount = new HashMap<>();

        nodeCount.put( "count", read.countsForNodeWithoutTxState( -1 ) );
        nodeCounts.add( nodeCount );

        tokens.labelsGetAllTokens().forEachRemaining( t -> {
            long count = read.countsForNodeWithoutTxState( t.id() );
            Map<String,Object> labelCount = new HashMap<>();
            labelCount.put( "label", anonymizer.label( t.name(), t.id() ) );
            labelCount.put( "count", count );
            nodeCounts.add( labelCount );
        } );

        return nodeCounts;
    }

    private static List<Map<String,Object>> relationshipCounts( TokenRead tokens, Read read, Anonymizer anonymizer )
    {
        List<Map<String,Object>> relationshipCounts = new ArrayList<>();
        Map<String,Object> relationshipCount = new HashMap<>();
        relationshipCount.put( "count", read.countsForRelationshipWithoutTxState( -1, -1, -1 ) );
        relationshipCounts.add( relationshipCount );

        List<NamedToken> labels = Iterators.asList( tokens.labelsGetAllTokens() );

        tokens.relationshipTypesGetAllTokens().forEachRemaining( t -> {
            long count = read.countsForRelationshipWithoutTxState( -1, t.id(), -1 );
            Map<String,Object> relationshipTypeCount = new HashMap<>();
            relationshipTypeCount.put( "relationshipType", anonymizer.relationshipType( t.name(), t.id() ) );
            relationshipTypeCount.put( "count", count );
            relationshipCounts.add( relationshipTypeCount );

            for ( NamedToken label : labels )
            {
                long startCount = read.countsForRelationshipWithoutTxState( label.id(), t.id(), -1 );
                if ( startCount > 0 )
                {
                    Map<String,Object> x = new HashMap<>();
                    x.put( "relationshipType", anonymizer.relationshipType( t.name(), t.id() ) );
                    x.put( "startLabel", anonymizer.label( label.name(), label.id() ) );
                    x.put( "count", startCount );
                    relationshipCounts.add( x );
                }
                long endCount = read.countsForRelationshipWithoutTxState( -1, t.id(), label.id() );
                if ( endCount > 0 )
                {
                    Map<String,Object> x = new HashMap<>();
                    x.put( "relationshipType", anonymizer.relationshipType( t.name(), t.id() ) );
                    x.put( "endLabel", anonymizer.label( label.name(), label.id() ) );
                    x.put( "count", endCount );
                    relationshipCounts.add( x );
                }
            }
        } );

        return relationshipCounts;
    }

    private static List<Map<String,Object>> indexes( TokenRead tokens, SchemaRead schemaRead, Anonymizer anonymizer )
            throws IndexNotFoundKernelException
    {
        List<Map<String,Object>> indexes = new ArrayList<>();

        SilentTokenNameLookup tokenLookup = new SilentTokenNameLookup( tokens );

        Iterator<IndexReference> iterator = schemaRead.indexesGetAll();
        while ( iterator.hasNext() )
        {
            IndexReference index = iterator.next();

            Map<String,Object> data = new HashMap<>();
            data.put( "labels", map( index.schema().getEntityTokenIds(),
                                     id -> anonymizer.label( tokenLookup.labelGetName( id ), id ) ) );

            data.put( "properties", map( index.schema().getPropertyIds(),
                                         id -> anonymizer.propertyKey( tokenLookup.propertyKeyGetName( id ), id ) ) );

            Register.DoubleLongRegister register = Registers.newDoubleLongRegister();
            schemaRead.indexUpdatesAndSize( index, register );
            data.put( "totalSize", register.readSecond() );
            data.put( "updatesSinceEstimation", register.readFirst() );
            schemaRead.indexSample( index, register );
            data.put( "estimatedUniqueSize", register.readFirst() );

            indexes.add( data );
        }

        return indexes;
    }

    private static List<Map<String,Object>> constraints( TokenRead tokens, SchemaRead schemaRead, Anonymizer anonymizer )
    {
        List<Map<String,Object>> constraints = new ArrayList<>();

        SilentTokenNameLookup tokenLookup = new SilentTokenNameLookup( tokens );

        Iterator<ConstraintDescriptor> iterator = schemaRead.constraintsGetAll();
        while ( iterator.hasNext() )
        {
            ConstraintDescriptor constraint = iterator.next();

            Map<String,Object> data = new HashMap<>();

            int labelId = constraint.schema().getEntityTokenIds()[0];
            data.put( "label", anonymizer.label( tokenLookup.labelGetName( labelId ), labelId ) );
            data.put( "properties", map( constraint.schema().getPropertyIds(),
                                         id -> anonymizer.propertyKey( tokenLookup.propertyKeyGetName( id ), id ) ) );
            data.put( "type", constraintType( constraint ) );

            constraints.add( data );
        }

        return constraints;
    }

    private static List<String> map( int[] ids, IntFunction<String> f )
    {
        return Arrays.stream(ids).mapToObj( f ).collect( Collectors.toList());
    }

    private static String constraintType( ConstraintDescriptor constraint )
    {
        switch ( constraint.type() )
        {
        case EXISTS:
            return "Existence constraint";
        case UNIQUE:
            return "Uniqueness constraint";
        case UNIQUE_EXISTS:
            return "Node Key";
        default:
            throw new IllegalArgumentException( "Unknown constraint type: " + constraint.type() );
        }
    }
}
