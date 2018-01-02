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
package org.neo4j.shell;

import java.rmi.RemoteException;

import org.junit.Test;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.kernel.ReadOnlyGraphDatabaseProxy;

public class TestReadOnlyServer extends AbstractShellTest
{
    @Override
    protected ShellServer newServer( GraphDatabaseAPI db ) throws ShellException, RemoteException
    {
        return new GraphDatabaseShellServer( new ReadOnlyGraphDatabaseProxy( db ) );
    }

    @Test
    public void executeReadCommands() throws Exception
    {
        Relationship[] rels = createRelationshipChain( 3 );
        executeCommand( "cd " + getStartNodeId(rels[0]) );
        executeCommand( "ls" );
        executeCommand( "cd " + getEndNodeId(rels[0]) );
        executeCommand( "ls", "<", ">" );
        executeCommand( "trav", "me" );
    }

    @Test
    public void readOnlyTransactionsShouldNotFail() throws Exception
    {
        Relationship[] rels = createRelationshipChain( 3 );
        executeCommand( "begin" );
        executeCommand( "cd " + getStartNodeId(rels[0]) );
        executeCommand( "ls" );
        executeCommand( "commit" );
    }

    private long getEndNodeId( Relationship rel )
    {
        try ( Transaction ignore = db.beginTx() )
        {
            return rel.getEndNode().getId();
        }
    }

    private long getStartNodeId( Relationship rel )
    {
        try ( Transaction ignore = db.beginTx() )
        {
            return rel.getStartNode().getId();
        }
    }

    @Test
    public void executeWriteCommands() throws Exception
    {
        executeCommandExpectingException( "mknode", "read only" );
    }
}
