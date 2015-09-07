/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.procedure.impl.js;

import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedures.ProcedureSignature;
import org.neo4j.kernel.api.procedures.ProcedureSource;
import org.neo4j.kernel.impl.util.SingleNodePath;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JavascriptProcedureMatchers
{
    public static final Node node = mock(Node.class);
    public static final Relationship rel = mock(Relationship.class);
    public static final Path path = new SingleNodePath( node );

    public static List<List<Object>> exec( ProcedureSignature sig, String script, Object ... args ) throws Throwable
    {
        Statement statement = mock(Statement.class);
        GraphDatabaseService gds = mock( GraphDatabaseService.class );

        // Basic stubs to access graph primitives, for type system tests
        when(gds.createNode()).thenReturn( node );
        when( node.createRelationshipTo( any( Node.class ), any( RelationshipType.class ) ) ).thenReturn( rel );

        final List<List<Object>> records = new LinkedList<>();
        new JavascriptLanguageHandler( ).compile( new ProcedureSource( sig, "squatchscript", script ) ).call( asList(args),
                new Visitor<List<Object>,ProcedureException>()
                {
                    @Override
                    public boolean visit( List<Object> record ) throws ProcedureException
                    {
                        records.add( new ArrayList<>(record) );
                        return true;
                    }
                } );

        return records;
    }

    public static Matcher<Iterable<? extends List<Object>>> yields( final Matcher<List<Object>>... records )
    {
        return contains( records );
    }

    public static Matcher<List<Object>> record( final Object ... expected )
    {
        return equalTo( asList( expected ) );
    }
}
