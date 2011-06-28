/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.junit.Test;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class TestApps extends AbstractShellTest
{
    @Test
    public void cd() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 3 );
        executeCommand( "cd" );
        executeCommand( "pwd", pwdOutputFor( relationships[0].getStartNode() ) );
        executeCommandExpectingException( "cd " + relationships[0].getStartNode().getId() );
        executeCommand( "pwd", pwdOutputFor( relationships[0].getStartNode() ) );
        executeCommand( "cd " + relationships[0].getEndNode().getId() );
        executeCommand( "pwd", pwdOutputFor( relationships[0].getStartNode(), relationships[0].getEndNode() ) );
        executeCommandExpectingException( "cd " + relationships[2].getEndNode().getId() );
        executeCommand( "pwd", pwdOutputFor( relationships[0].getStartNode(), relationships[0].getEndNode() ) );
        executeCommand( "cd -a " + relationships[2].getEndNode().getId() );
        executeCommand( "pwd", pwdOutputFor( relationships[0].getStartNode(), relationships[0].getEndNode(), relationships[2].getEndNode() ) );
        executeCommand( "cd .." );
        executeCommand( "pwd", pwdOutputFor( relationships[0].getStartNode(), relationships[0].getEndNode() ) );
        executeCommand( "cd " + relationships[1].getEndNode().getId() );
        executeCommand( "pwd", pwdOutputFor( relationships[0].getStartNode(), relationships[0].getEndNode(), relationships[1].getEndNode() ) );
    }
    
    @Test
    public void rmrelCanLeaveStrandedNodes() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 4 );
        executeCommand( "cd -a " + relationships[1].getEndNode().getId() );
        
        // Remove relationship with the check, shouldn't work
        Relationship relToDelete = relationships[2];
        executeCommandExpectingException( "rmrel -ed " + relToDelete.getId() );
        assertRelationshipExists( relToDelete );
        
        // Remove relationship without the check
        executeCommand( "rmrel -d " + relToDelete.getId() );
        assertRelationshipDoesnExist( relToDelete );
    }
    
    @Test
    public void pwdWorksOnDeletedNode() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 1 );
        executeCommand( "cd " + relationships[0].getEndNode().getId() );
        
        // Delete the relationship and node we're standing on
        Transaction tx = db.beginTx();
        relationships[0].getEndNode().delete();
        relationships[0].delete();
        tx.success();
        tx.finish();
        
        Relationship[] otherRelationships = createRelationshipChain( 1 );
        executeCommand( "pwd", "\\(0\\)-->\\(\\?\\)" );
        executeCommand( "cd -a " + otherRelationships[0].getEndNode().getId() );
        executeCommand( "ls" );
    }
}
