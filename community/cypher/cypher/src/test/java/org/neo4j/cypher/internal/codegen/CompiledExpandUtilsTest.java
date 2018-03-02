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
package org.neo4j.cypher.internal.codegen;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.cypher.internal.codegen.CompiledExpandUtils.nodeGetDegree;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

public class CompiledExpandUtilsTest
{
    @Rule
    public EmbeddedDatabaseRule db = new EmbeddedDatabaseRule();

    private Session session()
    {
        DependencyResolver resolver = this.db.getDependencyResolver();
        return resolver.resolveDependency( Kernel.class ).beginSession( LoginContext.AUTH_DISABLED );
    }

    @Test
    public void shouldComputeDegreeWithoutType() throws Exception
    {
        // GIVEN
        Session session = session();
        long node;
        try ( Transaction tx = session.beginTransaction() )
        {
            Write write = tx.dataWrite();
            node = write.nodeCreate();
            write.relationshipCreate( node,
                    tx.tokenWrite().relationshipTypeGetOrCreateForName( "R1" ),
                    write.nodeCreate() );
            write.relationshipCreate( node,
                    tx.tokenWrite().relationshipTypeGetOrCreateForName( "R2" ),
                    write.nodeCreate() );
            write.relationshipCreate( write.nodeCreate(),
                    tx.tokenWrite().relationshipTypeGetOrCreateForName( "R3" ),
                    node );
            write.relationshipCreate( node,
                    tx.tokenWrite().relationshipTypeGetOrCreateForName( "R4" ), node );

            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            Read read = tx.dataRead();
            CursorFactory cursors = tx.cursors();
            try ( NodeCursor nodes = cursors.allocateNodeCursor();
                  RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor() )
            {

                assertThat( nodeGetDegree( read, node, nodes, group, OUTGOING ), equalTo( 3 ) );
                assertThat( nodeGetDegree( read, node, nodes, group, INCOMING ), equalTo( 2 ) );
                assertThat( nodeGetDegree( read, node, nodes, group, BOTH ), equalTo( 4 ) );
            }
        }
    }

    @Test
    public void shouldComputeDegreeWithType() throws Exception
    {
        // GIVEN
        Session session = session();
        long node;
        int in, out, loop;
        try ( Transaction tx = session.beginTransaction() )
        {
            Write write = tx.dataWrite();
            node = write.nodeCreate();
            TokenWrite tokenWrite = tx.tokenWrite();
            out = tokenWrite.relationshipTypeGetOrCreateForName( "OUT" );
            in = tokenWrite.relationshipTypeGetOrCreateForName( "IN" );
            loop = tokenWrite.relationshipTypeGetOrCreateForName( "LOOP" );
            write.relationshipCreate( node,
                    out,
                    write.nodeCreate() );
            write.relationshipCreate( node, out, write.nodeCreate() );
            write.relationshipCreate( write.nodeCreate(), in, node );
            write.relationshipCreate( node, loop, node );

            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            Read read = tx.dataRead();
            CursorFactory cursors = tx.cursors();
            try ( NodeCursor nodes = cursors.allocateNodeCursor();
                  RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor() )
            {
                assertThat( nodeGetDegree( read, node, nodes, group, OUTGOING, out ), equalTo( 2 ) );
                assertThat( nodeGetDegree( read, node, nodes, group, OUTGOING, in ), equalTo( 0 ) );
                assertThat( nodeGetDegree( read, node, nodes, group, OUTGOING, loop ), equalTo( 1 ) );

                assertThat( nodeGetDegree( read, node, nodes, group, INCOMING, out ), equalTo( 0 ) );
                assertThat( nodeGetDegree( read, node, nodes, group, INCOMING, in ), equalTo( 1 ) );
                assertThat( nodeGetDegree( read, node, nodes, group, INCOMING, loop ), equalTo( 1 ) );

                assertThat( nodeGetDegree( read, node, nodes, group, BOTH, out ), equalTo( 2 ) );
                assertThat( nodeGetDegree( read, node, nodes, group, BOTH, in ), equalTo( 1 ) );
                assertThat( nodeGetDegree( read, node, nodes, group, BOTH, loop ), equalTo( 1 ) );
            }
        }
    }
}
