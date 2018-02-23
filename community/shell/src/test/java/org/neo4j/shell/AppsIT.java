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

import org.junit.Rule;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.neo4j.cypher.NodeStillHasRelationshipsException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.kernel.impl.transaction.TransactionStats;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.shell.impl.CollectingOutput;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.rule.SuppressOutput;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.function.Predicates.await;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.helpers.collection.MapUtil.genericMap;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.findNodesByLabelAndProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasLabels;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasSize;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.inTx;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.waitForIndex;

public class AppsIT extends AbstractShellIT
{
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void canSetPropertiesAndLsWithFilters() throws Exception
    {
        RelationshipType type1 = withName( "KNOWS" );
        RelationshipType type2 = withName( "LOVES" );
        Relationship[] relationships = createRelationshipChain( type1, 2 );
        Node node = getEndNode( relationships[0] );
        createRelationshipChain( node, type2, 1 );
        executeCommand( "cd " + node.getId() );
        executeCommand( "ls", "<-", "->" );
        executeCommand( "ls -p", "!Neo" );
        setProperty( node, "name", "Neo" );
        executeCommand( "ls -p", "Neo" );
        executeCommand( "ls", "<-", "->", "Neo", type1.name(), type2.name() );
        executeCommand( "ls -r", "<-", "->", "!Neo" );
        executeCommand( "ls -rf .*:out", "!<-", "->", "!Neo", type1.name(), type2.name() );
        executeCommand( "ls -rf .*:in", "<-", "!->", "!Neo", type1.name(), "!" + type2.name() );
        executeCommand( "ls -rf KN.*:in", "<-", "!->", type1.name(), "!" + type2.name() );
        executeCommand( "ls -rf LOVES:in", "!<-", "!->", "!" + type1.name(), "!" + type2.name() );
        executeCommand( "ls -pf something", "!<-", "!->", "!Neo" );
        executeCommand( "ls -pf name", "!<-", "!->", "Neo" );
        executeCommand( "ls -pf name:Something", "!<-", "!->", "!Neo" );
        executeCommand( "ls -pf name:Neo", "!<-", "!->", "Neo" );
    }

    @Test
    public void canSetAndRemoveProperties() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 2 );
        Node node = getEndNode( relationships[0] );
        executeCommand( "cd " + node.getId() );
        String name = "Mattias";
        executeCommand( "set name " + name );
        int age = 31;
        executeCommand( "set age -t int " + age );
        executeCommand( "set \"some property\" -t long[] \"[1234,5678]" );
        assertThat( node, inTx( db, hasProperty( "name" ).withValue( name ) ) );
        assertThat( node, inTx( db, hasProperty( "age" ).withValue( age ) ) );
        assertThat( node, inTx( db, hasProperty( "some property" ).withValue( new long[]{1234L, 5678L} ) ) );

        executeCommand( "rm age" );
        assertThat( node, inTx( db, not( hasProperty( "age" ) ) ) );
        assertThat( node, inTx( db, hasProperty( "name" ).withValue( name ) ) );
    }

    @Test
    public void canCreateRelationshipsAndNodes() throws Exception
    {
        RelationshipType type1 = withName( "type1" );
        RelationshipType type2 = withName( "type2" );
        RelationshipType type3 = withName( "type3" );

        executeCommand( "mknode --cd" );

        // No type supplied
        executeCommandExpectingException( "mkrel -c", "type" );

        executeCommand( "mkrel -ct " + type1.name() );
        Node node;
        try ( Transaction ignored = db.beginTx() )
        {
            Relationship relationship = db.getNodeById( 0 ).getSingleRelationship( type1, Direction.OUTGOING );
            node = relationship.getEndNode();
        }
        executeCommand( "mkrel -t " + type2.name() + " " + node.getId() );

        try ( Transaction ignored = db.beginTx() )
        {
            Relationship otherRelationship = db.getNodeById( 0 ).getSingleRelationship( type2, Direction.OUTGOING );
            assertEquals( node, otherRelationship.getEndNode() );
        }

        // With properties
        executeCommand( "mkrel -ct " + type3.name() + " --np \"{'name':'Neo','destiny':'The one'}\" --rp \"{'number':11}\"" );
        Node thirdNode;
        Relationship thirdRelationship;
        try ( Transaction ignored = db.beginTx() )
        {
            thirdRelationship = db.getNodeById( 0 ).getSingleRelationship( type3, Direction.OUTGOING );
            assertThat( thirdRelationship, inTx( db, hasProperty( "number" ).withValue( 11 ) ) );
            thirdNode = thirdRelationship.getEndNode();
        }
        assertThat( thirdNode, inTx( db, hasProperty( "name" ).withValue( "Neo" ) ) );
        assertThat( thirdNode, inTx( db, hasProperty( "destiny" ).withValue( "The one" ) ) );
        executeCommand( "cd -r " + thirdRelationship.getId() );
        executeCommand( "mv number other-number" );
        assertThat( thirdRelationship, inTx( db, not( hasProperty( "number" ) ) ) );
        assertThat( thirdRelationship, inTx( db, hasProperty( "other-number" ).withValue( 11 ) ) );

        // Create and go to
        executeCommand( "cd end" );
        executeCommand( "mkrel -ct " + type1.name() + " --np \"{'name':'new'}\" --cd" );
        executeCommand( "ls -p", "name", "new" );
    }

    @Test
    public void rmrelCanLeaveStrandedIslands() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 4 );
        executeCommand( "cd -a " + getEndNode( relationships[1] ).getId() );

        Relationship relToDelete = relationships[2];

        Node otherNode = getEndNode( relToDelete );
        executeCommand( "rmrel -fd " + relToDelete.getId() );
        assertRelationshipDoesntExist( relToDelete );
        assertNodeExists( otherNode );
    }

    @Test
    public void rmrelCanLeaveStrandedNodes() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 1 );
        Node otherNode = getEndNode( relationships[0] );

        executeCommand( "cd 0" );

        executeCommand( "rmrel -f " + relationships[0].getId() );
        assertRelationshipDoesntExist( relationships[0] );
        assertNodeExists( otherNode );
    }

    @Test
    public void rmrelCanDeleteStrandedNodes() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 1 );
        Node otherNode = getEndNode( relationships[0] );

        executeCommand( "cd 0" );
        executeCommand( "rmrel -fd " + relationships[0].getId(), "not having any relationships" );
        assertRelationshipDoesntExist( relationships[0] );
        assertNodeDoesntExist( otherNode );
    }

    @Test
    public void rmrelCanDeleteRelationshipSoThatCurrentNodeGetsStranded() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 2 );
        executeCommand( "cd " + getEndNode( relationships[0] ).getId() );
        deleteRelationship( relationships[0] );
        Node currentNode = getStartNode( relationships[1] );
        executeCommand( "rmrel -fd " + relationships[1].getId(), "not having any relationships" );
        assertNodeExists( currentNode );

        try ( Transaction ignored = db.beginTx() )
        {
            assertFalse( currentNode.hasRelationship() );
        }
    }

    private Node getStartNode( Relationship relationship )
    {
        beginTx();
        try
        {
            return relationship.getStartNode();
        }
        finally
        {
            finishTx( false );
        }
    }

    @Test
    public void rmnodeCanDeleteStrandedNodes() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 1 );
        Node strandedNode = getEndNode( relationships[0] );
        deleteRelationship( relationships[0] );
        executeCommand( "rmnode " + strandedNode.getId() );
        assertNodeDoesntExist( strandedNode );
    }

    @Test
    public void rmnodeCanDeleteConnectedNodes() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 2 );
        Node middleNode = getEndNode( relationships[0] );
        executeCommandExpectingException( "rmnode " + middleNode.getId(), "still has relationships" );
        assertNodeExists( middleNode );
        Node endNode = getEndNode( relationships[1] );
        executeCommand( "rmnode -f " + middleNode.getId(), "deleted" );
        assertNodeDoesntExist( middleNode );
        assertRelationshipDoesntExist( relationships[0] );
        assertRelationshipDoesntExist( relationships[1] );

        assertNodeExists( endNode );
        executeCommand( "cd -a " + endNode.getId() );
        executeCommand( "rmnode " + endNode.getId() );
        executeCommand( "pwd", Pattern.quote( "(?)" ) );
    }

    private Node getEndNode( Relationship relationship )
    {
        beginTx();
        try
        {
            return relationship.getEndNode();
        }
        finally
        {
            finishTx( false );
        }
    }

    @Test
    public void pwdWorksOnDeletedNode() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 1 );
        executeCommand( "cd " + getEndNode( relationships[0] ).getId() );

        // Delete the relationship and node we're standing on
        beginTx();
        relationships[0].getEndNode().delete();
        relationships[0].delete();
        finishTx();

        Relationship[] otherRelationships = createRelationshipChain( 1 );
        executeCommand( "pwd", "Current is .+" );
        executeCommand( "cd -a " + getEndNode( otherRelationships[0] ).getId() );
        executeCommand( "ls" );
    }

    @Test
    public void startEvenIfReferenceNodeHasBeenDeleted() throws Exception
    {
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            String name = "Test";
            node.setProperty( "name", name );
            tx.success();
        }

        GraphDatabaseShellServer server = new GraphDatabaseShellServer( db );
        ShellClient client = newShellClient( server );
        executeCommand( client, "pwd", Pattern.quote( "(?)" ) );
        executeCommand( client, "ls " + node.getId(), "Test" );
        executeCommand( client, "cd -a " + node.getId() );
        executeCommand( client, "ls", "Test" );
    }

    @Test
    public void cypherWithSelfParameter() throws Exception
    {
        String nodeOneName = "Node ONE";
        String name = "name";
        String nodeTwoName = "Node TWO";
        String relationshipName = "The relationship";

        beginTx();
        Node node = db.createNode();
        node.setProperty( name, nodeOneName );
        Node otherNode = db.createNode();
        otherNode.setProperty( name, nodeTwoName );
        Relationship relationship = node.createRelationshipTo( otherNode, RELATIONSHIP_TYPE );
        relationship.setProperty( name, relationshipName );
        Node strayNode = db.createNode();
        finishTx();

        executeCommand( "cd -a " + node.getId() );
        executeCommand( "MATCH (n) WHERE n = {self} RETURN n.name;", nodeOneName );
        executeCommand( "cd -r " + relationship.getId() );
        executeCommand( "MATCH ()-[r]->() WHERE r = {self} RETURN r.name;", relationshipName );
        executeCommand( "cd " + otherNode.getId() );
        executeCommand( "MATCH (n) WHERE n = {self} RETURN n.name;", nodeTwoName );

        executeCommand( "cd -a " + strayNode.getId() );
        beginTx();
        strayNode.delete();
        finishTx();
        executeCommand( "MATCH (n) WHERE id(n) = " + node.getId() + " RETURN n.name;", nodeOneName );
    }

    @Test
    public void cypherTiming() throws Exception
    {
        beginTx();
        Node node = db.createNode();
        Node otherNode = db.createNode();
        node.createRelationshipTo( otherNode, RELATIONSHIP_TYPE );
        finishTx();

        executeCommand( "MATCH (n) WHERE id(n) = " + node.getId() + " optional match p=(n)-[r*]-(m) RETURN p;",
                "\\d+ ms", "1 row" );
    }

    @Test
    public void filterProperties() throws Exception
    {
        beginTx();
        Node node = db.createNode();
        node.setProperty( "name", "Mattias" );
        node.setProperty( "blame", "Someone else" );
        finishTx();

        executeCommand( "cd -a " + node.getId() );
        executeCommand( "ls", "Mattias" );
        executeCommand( "ls -pf name", "Mattias", "!Someone else" );
        executeCommand( "ls -f name", "Mattias", "!Someone else" );
        executeCommand( "ls -f blame", "!Mattias", "Someone else" );
        executeCommand( "ls -pf .*ame", "Mattias", "Someone else" );
        executeCommand( "ls -f .*ame", "Mattias", "Someone else" );
    }

    @Test
    public void createNewNode() throws Exception
    {
        executeCommand( "mknode --np \"{'name':'test'}\" --cd" );
        executeCommand( "ls", "name", "test", "!-" /*no relationship*/ );
        executeCommand( "mkrel -t KNOWS 0" );
        executeCommand( "ls", "name", "test", "-", "KNOWS" );
    }

    @Test
    public void createNodeWithArrayProperty() throws Exception
    {
        executeCommand( "mknode --np \"{'values':[1,2,3,4]}\" --cd" );
        assertThat( getCurrentNode(), inTx( db, hasProperty( "values" ).withValue( new int[]{1, 2, 3, 4} ) ) );
    }

    @Test
    public void createNodeWithLabel() throws Exception
    {
        executeCommand( "mknode --cd -l Person" );
        assertThat( getCurrentNode(), inTx( db, hasLabels( "Person" ) ) );
    }

    @Test
    public void createNodeWithColonPrefixedLabel() throws Exception
    {
        executeCommand( "mknode --cd -l :Person" );
        assertThat( getCurrentNode(), inTx( db, hasLabels( "Person" ) ) );
    }

    @Test
    public void createNodeWithPropertiesAndLabels() throws Exception
    {
        executeCommand( "mknode --cd --np \"{'name': 'Test'}\" -l \"['Person', ':Thing']\"" );

        assertThat( getCurrentNode(), inTx( db, hasProperty( "name" ).withValue( "Test" ) ) );
        assertThat( getCurrentNode(), inTx( db, hasLabels( "Person", "Thing" ) ) );
    }

    @Test
    public void createRelationshipWithArrayProperty() throws Exception
    {
        String type = "ARRAY";
        executeCommand( "mknode --cd" );
        executeCommand( "mkrel -ct " + type + " --rp \"{'values':[1,2,3,4]}\"" );

        try ( Transaction ignored = db.beginTx() )
        {
            assertThat( getCurrentNode().getSingleRelationship( withName( type ), OUTGOING ), inTx( db, hasProperty(
                    "values" ).withValue( new int[]{1, 2, 3, 4} ) ) );
        }
    }

    @Test
    public void createRelationshipToNewNodeWithLabels() throws Exception
    {
        String type = "TEST";
        executeCommand( "mknode --cd" );
        executeCommand( "mkrel -ctl " + type + " Person" );

        try ( Transaction ignored = db.beginTx() )
        {
            assertThat( getCurrentNode().getSingleRelationship(
                    withName( type ), OUTGOING ).getEndNode(), inTx( db, hasLabels( "Person" ) ) );
        }
    }

    @Test
    public void getDbinfo() throws Exception
    {
        // It's JSON coming back from dbinfo command
        executeCommand( "dbinfo -g Kernel", "\\{", "\\}", "StoreId" );
    }

    @Test
    public void canReassignShellVariables() throws Exception
    {
        executeCommand( "export a=10" );
        executeCommand( "export b=a" );
        executeCommand( "env", "a=10", "b=10" );
    }

    @Test
    public void canSetVariableToMap() throws Exception
    {
        executeCommand( "export a={a:10}" );
        executeCommand( "export b={\"b\":\"foo\"}" );
        executeCommand( "env", "a=\\{a=10\\}", "b=\\{b=foo\\}" );
    }

    @Test
    public void canSetVariableToScalars() throws Exception
    {
        executeCommand( "export a=true" );
        executeCommand( "export b=100" );
        executeCommand( "export c=\"foo\"" );
        executeCommand( "env", "a=true", "b=100", "c=foo" );
    }

    @Test
    public void canSetVariableToArray() throws Exception
    {
        executeCommand( "export a=[1,true,\"foo\"]" );
        executeCommand( "env", "a=\\[1, true, foo\\]" );
    }

    @Test
    public void canRemoveShellVariables() throws Exception
    {
        executeCommand( "export a=10" );
        executeCommand( "export a=null" );
        executeCommand( "env", "!a=10", "!a=null" );
    }

    @Test
    public void canUseAlias() throws Exception
    {
        executeCommand( "alias x=pwd" );
        executeCommand( "x", "Current is .+" );
    }

    @Test
    public void cypherNodeStillHasRelationshipsException() throws Exception
    {
        // Given
        executeCommand( "create (a),(b),(a)-[:x]->(b);" );

        String stackTrace = "";
        // When
        try
        {
            executeCommand( "match (n) delete n;" );
            fail( "Should have failed with " + NodeStillHasRelationshipsException.class.getName() + " exception" );
        }
        catch ( ShellException e )
        {
            stackTrace = e.getStackTraceAsString();
        }

        // Then
        assertThat( stackTrace, containsString( "still has relationships" ) );
    }

    @Test
    public void startCypherQueryWithUnwind() throws Exception
    {
        executeCommand( "unwind [1,2,3] as x return x;", "| x |", "| 1 |" );
    }

    @Test
    public void useCypherMerge() throws Exception
    {
        executeCommand( "merge (n:Person {name:'Andres'});" );

        assertThat( findNodesByLabelAndProperty( label( "Person" ), "name", "Andres", db ), hasSize( 1 ) );
    }

    @Test
    public void useCypherPeriodicCommit() throws Exception
    {
        File file = File.createTempFile( "file", "csv", null );
        try ( PrintWriter writer = new PrintWriter( file ) )
        {
            String url = file.toURI().toURL().toString().replace( "\\", "\\\\" );
            writer.println( "1,2,3" );
            writer.println( "4,5,6" );
            writer.close();

            // WHEN
            executeCommand( "USING PERIODIC COMMIT 100 LOAD CSV FROM '" + url + "' AS line CREATE ();",
                    "Nodes created: 2" );
        }
        catch ( ShellException e )
        {
            // THEN NOT
            fail( "Failed to execute PERIODIC COMMIT query" );
        }
        finally
        {
            file.delete();
        }
    }

    @Test
    public void canSetInitialSessionVariables() throws Exception
    {
        Map<String, Serializable> values = genericMap( "mykey", "myvalue",
                "my_other_key", "My other value" );
        ShellClient client = newShellClient( shellServer, values );
        String[] allStrings = new String[values.size() * 2];
        int i = 0;
        for ( Map.Entry<String, Serializable> entry : values.entrySet() )
        {
            allStrings[i++] = entry.getKey();
            allStrings[i++] = entry.getValue().toString();
        }
        executeCommand( client, "env", allStrings );
    }

    @Test
    public void canDisableWelcomeMessage() throws Exception
    {
        Map<String, Serializable> values = genericMap( "quiet", "true" );
        final CollectingOutput out = new CollectingOutput();
        ShellClient client = new SameJvmClient( values, shellServer, out );
        client.shutdown();
        final String outString = out.asString();
        assertEquals( false, outString.contains( "Welcome to the Neo4j Shell! Enter 'help' for a list of commands" ),
                "Shows welcome message: " + outString );
    }

    @Test
    public void doesShowWelcomeMessage() throws Exception
    {
        Map<String, Serializable> values = genericMap();
        final CollectingOutput out = new CollectingOutput();
        ShellClient client = new SameJvmClient( values, shellServer, out );
        client.shutdown();
        final String outString = out.asString();
        assertEquals( true, outString.contains( "Welcome to the Neo4j Shell! Enter 'help' for a list of commands" ),
                "Shows welcome message: " + outString );
    }

    @Test
    public void canExecuteCypherWithShellVariables() throws Exception
    {
        Map<String, Serializable> variables = genericMap( "id", 0 );
        ShellClient client = newShellClient( shellServer, variables );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        executeCommand( client, "match (n) where id(n) = {id} return n;", "1 row" );
    }

    @Test
    public void canDumpSubgraphWithCypher() throws Exception
    {
        final RelationshipType type = withName( "KNOWS" );
        beginTx();
        createRelationshipChain( db.createNode(), type, 1 );
        finishTx();
        executeCommand( "dump match (n)-[r]->(m) where id(n) = 0 return n,r,m;",
                "begin",
                "create \\(_0\\)",
                "create \\(_1\\)",
                "\\(_0\\)-\\[:`KNOWS`\\]->\\(_1\\)",
                "commit" );
    }

    @Test
    public void canDumpGraph() throws Exception
    {
        final RelationshipType type = withName( "KNOWS" );
        beginTx();
        final Relationship rel = createRelationshipChain( db.createNode(), type, 1 )[0];
        rel.getStartNode().setProperty( "f o o", "bar" );
        rel.setProperty( "since", 2010 );
        rel.getEndNode().setProperty( "flags", new Boolean[]{true, false, true} );
        finishTx();
        executeCommand( "dump ",
                "begin",
                "create \\(_0 \\{\\`f o o\\`:\"bar\"\\}\\)",
                "create \\(_1 \\{`flags`:\\[true, false, true\\]\\}\\)",
                "\\(_0\\)-\\[:`KNOWS` \\{`since`:2010\\}\\]->\\(_1\\)",
                "commit"
        );
    }

    @Test
    public void commentsAreIgnored() throws Exception
    {
        // See GitHub issue #1204
        executeCommand( "// a comment\n" );
    }

    @Test
    public void canAddLabelToNode() throws Exception
    {
        // GIVEN
        Relationship[] chain = createRelationshipChain( 1 );
        Node node = getEndNode( chain[0] );
        executeCommand( "cd -a " + node.getId() );

        // WHEN
        executeCommand( "set -l Person" );

        // THEN
        assertThat( node, inTx( db, hasLabels( "Person" ) ) );
    }

    @Test
    public void canAddMultipleLabelsToNode() throws Exception
    {
        // GIVEN
        Relationship[] chain = createRelationshipChain( 1 );
        Node node = getEndNode( chain[0] );
        executeCommand( "cd -a " + node.getId() );

        // WHEN
        executeCommand( "set -l ['Person','Thing']" );

        // THEN
        assertThat( node, inTx( db, hasLabels( "Person", "Thing" ) ) );
    }

    @Test
    public void canRemoveLabelFromNode() throws Exception
    {
        // GIVEN
        beginTx();
        Relationship[] chain = createRelationshipChain( 1 );
        Node node = chain[0].getEndNode();
        node.addLabel( label( "Person" ) );
        node.addLabel( label( "Pilot" ) );
        finishTx();
        executeCommand( "cd -a " + node.getId() );

        // WHEN
        executeCommand( "rm -l Person" );

        // THEN
        assertThat( node, inTx( db, hasLabels( "Pilot" ) ) );
        assertThat( node, inTx( db, not( hasLabels( "Person" ) ) ) );
    }

    @Test
    public void canRemoveMultipleLabelsFromNode() throws Exception
    {
        // GIVEN
        beginTx();
        Relationship[] chain = createRelationshipChain( 1 );
        Node node = chain[0].getEndNode();
        node.addLabel( label( "Person" ) );
        node.addLabel( label( "Thing" ) );
        node.addLabel( label( "Object" ) );
        finishTx();
        executeCommand( "cd -a " + node.getId() );

        // WHEN
        executeCommand( "rm -l ['Person','Object']" );

        // THEN
        assertThat( node, inTx( db, hasLabels( "Thing" ) ) );
        assertThat( node, inTx( db, not( hasLabels( "Person", "Object" ) ) ) );
    }

    @Test
    public void canListLabels() throws Exception
    {
        // GIVEN
        beginTx();
        Relationship[] chain = createRelationshipChain( 1 );
        Node node = chain[0].getEndNode();
        node.addLabel( label( "Person" ) );
        node.addLabel( label( "Father" ) );
        finishTx();
        executeCommand( "cd -a " + node.getId() );

        // WHEN/THEN
        executeCommand( "ls", ":Person", ":Father" );
    }

    @Test
    public void canListFilteredLabels() throws Exception
    {
        // GIVEN
        beginTx();
        Relationship[] chain = createRelationshipChain( 1 );
        Node node = chain[0].getEndNode();
        node.addLabel( label( "Person" ) );
        node.addLabel( label( "Father" ) );
        finishTx();
        executeCommand( "cd -a " + node.getId() );

        // WHEN/THEN
        executeCommand( "ls -f Per.*", ":Person", "!:Father" );
    }

    @Test
    public void canListIndexes() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        beginTx();
        IndexDefinition index = db.schema().indexFor( label ).on( "name" ).create();
        finishTx();
        waitForIndex( db, index );

        // WHEN / THEN
        executeCommand( "schema ls", ":Person", IndexState.ONLINE.name() );
    }

    @Test
    public void canListIndexesForGivenLabel() throws Exception
    {
        // GIVEN
        Label label1 = label( "Person" );
        Label label2 = label( "Building" );
        beginTx();
        IndexDefinition index1 = db.schema().indexFor( label1 ).on( "name" ).create();
        IndexDefinition index2 = db.schema().indexFor( label2 ).on( "name" ).create();
        finishTx();
        waitForIndex( db, index1 );
        waitForIndex( db, index2 );

        // WHEN / THEN
        executeCommand( "schema ls -l " + label2.name(), ":" + label2.name(),
                IndexState.ONLINE.name(), "!:" + label1.name() );
    }

    @Test
    public void canListIndexesForGivenPropertyAndLabel() throws Exception
    {
        // GIVEN
        Label label1 = label( "Person" );
        Label label2 = label( "Thing" );
        String property1 = "name";
        String property2 = "age";
        beginTx();
        IndexDefinition index1 = db.schema().indexFor( label1 ).on( property1 ).create();
        IndexDefinition index2 = db.schema().indexFor( label1 ).on( property2 ).create();
        IndexDefinition index3 = db.schema().indexFor( label2 ).on( property1 ).create();
        IndexDefinition index4 = db.schema().indexFor( label2 ).on( property2 ).create();
        finishTx();
        waitForIndex( db, index1 );
        waitForIndex( db, index2 );
        waitForIndex( db, index3 );
        waitForIndex( db, index4 );

        // WHEN / THEN
        executeCommand( "schema ls" +
                " -l :" + label2.name() +
                " -p " + property1,

                label2.name(), property1, "!" + label1.name(), "!" + property2 );
    }

    @Test
    public void canAwaitIndexesToComeOnline() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        beginTx();
        IndexDefinition index = db.schema().indexFor( label ).on( "name" ).create();
        finishTx();

        // WHEN / THEN
        executeCommand( "schema await -l " + label.name() );
        beginTx();
        assertEquals( IndexState.ONLINE, db.schema().getIndexState( index ) );
        finishTx();
    }

    @Test
    public void canListIndexesWhenNoOptionGiven() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        String property = "name";
        beginTx();
        IndexDefinition index = db.schema().indexFor( label ).on( property ).create();
        finishTx();
        waitForIndex( db, index );

        // WHEN / THEN
        executeCommand( "schema", label.name(), property );
    }

    @Test
    public void canListUniquePropertyConstraints() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        beginTx();
        db.schema().constraintFor( label ).assertPropertyIsUnique( "name" ).create();
        finishTx();

        // WHEN / THEN
        executeCommand( "schema ls", "ON \\(person:Person\\) ASSERT person.name IS UNIQUE" );
    }

    @Test
    public void canListUniquePropertyConstraintsByLabel() throws Exception
    {
        // GIVEN
        Label label1 = label( "Person" );
        beginTx();
        db.schema().constraintFor( label1 ).assertPropertyIsUnique( "name" ).create();
        finishTx();

        // WHEN / THEN
        executeCommand( "schema ls -l :Person", "ON \\(person:Person\\) ASSERT person.name IS UNIQUE" );
    }

    @Test
    public void canListUniquePropertyConstraintsByLabelAndProperty() throws Exception
    {
        // GIVEN
        Label label1 = label( "Person" );
        beginTx();
        db.schema().constraintFor( label1 ).assertPropertyIsUnique( "name" ).create();
        finishTx();

        // WHEN / THEN
        executeCommand( "schema ls -l :Person -p name", "ON \\(person:Person\\) ASSERT person.name IS UNIQUE" );
    }

    @Test
    public void failSampleWhenNoOptionGiven() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        String property = "name";
        beginTx();
        IndexDefinition index = db.schema().indexFor( label ).on( property ).create();
        finishTx();
        waitForIndex( db, index );

        // WHEN / THEN
        try
        {
            executeCommand( "schema sample");
            fail("This should fail");
        }
        catch ( ShellException e )
        {
            assertThat(e.getMessage(), containsString("Invalid usage of sample"));
        }
    }

    @Test
    public void canSampleAllIndexes() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        String property = "name";
        beginTx();
        IndexDefinition index = db.schema().indexFor( label ).on( property ).create();
        finishTx();
        waitForIndex( db, index );

        // WHEN / THEN
        executeCommand( "schema sample -a");
    }

    @Test
    public void canForceSampleIndexes() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        String property = "name";
        beginTx();
        IndexDefinition index = db.schema().indexFor( label ).on( property ).create();
        finishTx();
        waitForIndex( db, index );

        // WHEN / THEN
        executeCommand( "schema sample -a -f");
    }

    @Test
    public void canSampleSpecificIndex() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        String property = "name";
        beginTx();
        IndexDefinition index = db.schema().indexFor( label ).on( property ).create();
        finishTx();
        waitForIndex( db, index );

        // WHEN / THEN
        executeCommand( "schema sample -l Person -p name");
    }

    @Test
    public void failSamplingWhenProvidingBadLabel() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        String property = "name";
        beginTx();
        IndexDefinition index = db.schema().indexFor( label ).on( property ).create();
        finishTx();
        waitForIndex( db, index );

        // WHEN / THEN
        try
        {
            executeCommand( "schema sample -l People -p name");
            fail("This should fail");
        }
        catch ( ShellException e )
        {
            assertThat(e.getMessage(), containsString("No label associated with 'People' was found"));
        }
    }

    @Test
    public void failSamplingWhenProvidingBadProperty() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        String property = "name";
        beginTx();
        IndexDefinition index = db.schema().indexFor( label ).on( property ).create();
        finishTx();
        waitForIndex( db, index );

        // WHEN / THEN
        try
        {
            executeCommand( "schema sample -l Person -p namn");
            fail("This should fail");
        }
        catch ( ShellException e )
        {
            assertThat(e.getMessage(), containsString("No property associated with 'namn' was found"));
        }
    }

    @Test
    public void failSamplingWhenProvidingOnlyLabel() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        String property = "name";
        beginTx();
        IndexDefinition index = db.schema().indexFor( label ).on( property ).create();
        finishTx();
        waitForIndex( db, index );

        // WHEN / THEN
        try
        {
            executeCommand( "schema sample -l Person");
            fail("This should fail");
        }
        catch ( ShellException e )
        {
            assertThat(e.getMessage(), containsString("Provide both the property and the label, or run with -a to sample all indexes"));
        }
    }

    @Test
    public void failSamplingWhenProvidingOnlyProperty() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        String property = "name";
        beginTx();
        IndexDefinition index = db.schema().indexFor( label ).on( property ).create();
        finishTx();
        waitForIndex( db, index );

        // WHEN / THEN
        try
        {
            executeCommand( "schema sample -p name");
            fail("This should fail");
        }
        catch ( ShellException e )
        {
            assertThat(e.getMessage(), containsString("Provide both the property and the label, or run with -a to sample all indexes"));
        }
    }

    @Test
    public void failSamplingWhenProvidingMultipleLabels() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        Label otherLabel = label( "Dog" );
        String property = "name";
        beginTx();
        IndexDefinition index1 = db.schema().indexFor( label ).on( property ).create();
        IndexDefinition index2 = db.schema().indexFor( otherLabel ).on( property ).create();
        finishTx();
        waitForIndex( db, index1 );
        waitForIndex( db, index2 );

        // WHEN / THEN
        try
        {
            executeCommand( "schema sample -p name -l [Person,Dog]");
            fail("This should fail");
        }
        catch ( ShellException e )
        {
            assertThat(e.getMessage(), containsString("Only one label must be provided"));
        }
    }

    @Test
    public void committingFailedTransactionShouldProperlyFinishTheTransaction() throws Exception
    {
        // GIVEN a transaction with a created constraint in it
        executeCommand( "begin" );
        executeCommand( "create constraint on (node:Label1) assert node.key1 is unique;" );

        // WHEN trying to do a data update
        try
        {
            executeCommand( "mknode --cd" );
            fail( "Should have failed" );
        }
        catch ( ShellException e )
        {
            assertThat( e.getMessage(), containsString( ConstraintViolationException.class.getSimpleName() ) );
            assertThat( e.getMessage(), containsString( "Cannot perform data updates" ) );
        }

        // THEN the commit should fail afterwards
        try
        {
            executeCommand( "commit" );
            fail( "Commit should fail" );
        }
        catch ( ShellException e )
        {
            assertThat( e.getMessage(), containsString( "rolled back" ) );
        }
        // and also a rollback following it should fail
        try
        {
            executeCommand( "rollback" );
            fail( "Rolling back at this point should fail since there's no transaction open" );
        }
        catch ( ShellException e )
        {
            assertThat( e.getMessage(), containsString( "Not in a transaction" ) );
        }
    }

    @Test
    public void allowsArgumentsStartingWithSingleHyphensForCommandsThatDontTakeOptions() throws Exception
    {
        executeCommand( "CREATE (n { test : ' -0' });" );
    }

    @Test
    public void allowsArgumentsStartingWithDoubldHyphensForCommandsThatDontTakeOptions() throws Exception
    {
        executeCommand( "MATCH () -- () RETURN 0;" );
    }

    @Test
    public void allowsCypherToContainExclamationMarks() throws Exception
    {
        executeCommand( "RETURN \"a\"+\"!b\";", "a!b" );
    }

    @Test
    public void shouldAllowQueriesToStartWithOptionalMatch() throws Exception
    {
        executeCommand( "OPTIONAL MATCH (n) RETURN n;" );
    }

    @Test
    public void shouldAllowExplainAsStartForACypherQuery() throws Exception
    {
        executeCommand( "EXPLAIN OPTIONAL MATCH (n) RETURN n;", "No data returned" );
    }

    @Test
    public void shouldAllowProfileAsStartForACypherQuery() throws Exception
    {
        executeCommand( "PROFILE MATCH (n) RETURN n;", "DB Hits" );
    }

    @Test
    public void shouldAllowPlannerAsStartForACypherQuery() throws Exception
    {

        executeCommand( "CYPHER planner=cost MATCH (n) RETURN n;" );
    }

    @Test
    public void canListAllConfiguration() throws Exception
    {
        executeCommand( "dbinfo -g Configuration", "\"dbms.record_format\": \"\"" );
    }

    @Test
    public void canTerminateAnActiveCommand() throws Exception
    {
        TransactionStats txStats = db.getDependencyResolver().resolveDependency( TransactionStats.class );
        assertEquals( 0, txStats.getNumberOfActiveTransactions() );

        createNodeAndLockItInDifferentThread( "Person", "id", 42 );

        Serializable clientId = shellClient.getId();
        Future<?> result = runAsync( () ->
        {
            try
            {
                // await 2 active transactions: one that locked node and one that wants to update property via cypher
                await( () -> txStats.getNumberOfActiveTransactions() == 2, 1, MINUTES );
                shellServer.terminate( clientId );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        } );

        executeCommandExpectingException( "MATCH (p:Person {id: 42}) SET p.id = 24 RETURN p;", "has been terminated" );
        assertNull( result.get() );
    }

    @Test
    public void canUseForeach() throws Exception
    {
        executeCommand( "FOREACH(x in range(0,10) | CREATE ());" );
    }

    @Test
    public void use_cypher_periodic_commit2() throws Exception
    {
        File file = File.createTempFile( "file", "csv", null );
        try ( PrintWriter writer = new PrintWriter( file ) )
        {
            String url = file.toURI().toURL().toString().replace("\\", "\\\\");
            writer.println("apa,2,3");
            writer.println("4,5,6");
            writer.close();

            // WHEN
            executeCommand( "USING PERIODIC COMMIT 100 " +
                            "LOAD CSV FROM '" + url + "' AS line " +
                            "CREATE () " +
                            "RETURN line;", "apa" );
        }
        catch ( ShellException e )
        {
            // THEN NOT
            fail( "Failed to execute PERIODIC COMMIT query" );
        }
        finally
        {
            file.delete();
        }
    }

    @Test
    public void canUseCall() throws Exception
    {
        executeCommand( "CALL db.labels" );
    }

    @Test
    public void travMustListAllPathsWithinDistance() throws Exception
    {
        long me;
        RelationshipType type;
        long firstOut;
        long firstOutOut;
        long secondOut;
        long secondOutOut;
        try ( Transaction tx = db.beginTx() )
        {
            Relationship[] firstChain = createRelationshipChain( 2 );
            Node startNode = firstChain[0].getStartNode();
            type = firstChain[0].getType();
            Relationship[] secondChain = createRelationshipChain( startNode, type, 3 );
            me = startNode.getId();
            firstOut = firstChain[0].getEndNodeId();
            firstOutOut = firstChain[1].getEndNodeId();
            secondOut = secondChain[0].getEndNodeId();
            secondOutOut = secondChain[1].getEndNodeId();
            tx.success();
        }
        String pb = "\\(";
        String pe = "\\)";
        String rel = pe + "-\\[:" + type + "\\]->" + pb;
        executeCommand( "cd " + me );
        executeCommand( "trav -d 2",
                pb + "me" + pe,
                pb + "me" + rel + firstOut + pe,
                pb + "me" + rel + firstOut + rel + firstOutOut + pe,
                pb + "me" + rel + secondOut + pe,
                pb + "me" + rel + secondOut + rel + secondOutOut + pe );
    }

    @Test
    public void travMustRunCommandForAllPaths() throws Exception
    {
        long me;
        RelationshipType type;
        long firstOut;
        long firstOutOut;
        long secondOut;
        long secondOutOut;
        long secondOutOutOut;
        try ( Transaction tx = db.beginTx() )
        {
            Relationship[] firstChain = createRelationshipChain( 2 );
            Node startNode = firstChain[0].getStartNode();
            type = firstChain[0].getType();
            Relationship[] secondChain = createRelationshipChain( startNode, type, 3 );
            me = startNode.getId();
            firstOut = firstChain[0].getEndNodeId();
            firstOutOut = firstChain[1].getEndNodeId();
            secondOut = secondChain[0].getEndNodeId();
            secondOutOut = secondChain[1].getEndNodeId();
            secondOutOutOut = secondChain[2].getEndNodeId();
            tx.success();
        }
        String pb = "\\(";
        String pe = "\\)";
        String rel = pe + "-\\[:" + type + "\\]->" + pb;
        executeCommand( "cd " + me );
        executeCommand( "trav -d 2 -c \"ls $i\" ",
                pb + "me" + rel + firstOut + pe,
                pb + firstOut + rel + firstOutOut + pe,
                pb + "me" + rel + secondOut + pe,
                pb + secondOut + rel + secondOutOut + pe,
                pb + secondOutOut + rel + secondOutOutOut + pe );
    }

    @Test
    public void shouldSupportUsingPeriodicCommitInSession() throws Exception
    {
        long fileSize = 120;
        long batch = 40;

        String csvFileUrl = createCsvFile( fileSize );
        long expectedCommitCount = fileSize / batch;

        verifyNumberOfCommits( "USING PERIODIC COMMIT " + batch + " LOAD CSV FROM '" + csvFileUrl + "' AS l CREATE ();",
                expectedCommitCount );
    }

    @Test
    public void shouldSupportUsingPeriodicCommitInMultipleLine() throws Exception
    {
        long fileSize = 120;
        long batch = 40;

        String csvFileUrl = createCsvFile( fileSize );
        long expectedCommitCount = fileSize / batch;

        verifyNumberOfCommits(
                "USING\nPERIODIC\nCOMMIT\n" + batch + "\nLOAD\nCSV\nFROM '" + csvFileUrl + "' AS l\nCREATE ();",
                expectedCommitCount );
    }

    private void verifyNumberOfCommits( String query, long expectedCommitCount )
    {
        // Given

        CtrlCHandler ctrlCHandler = mock( CtrlCHandler.class );
        StartClient startClient = getStartClient();
        long txIdBeforeQuery = lastClosedTxId();

        // When
        startClient.start( new String[]{"-path", db.getStoreDir().getAbsolutePath(), "-c", query}, ctrlCHandler );

        // then
        long txId = lastClosedTxId();
        assertEquals( expectedCommitCount + txIdBeforeQuery + 1 /* shell opens a tx to show the prompt */, txId );

    }

    private StartClient getStartClient()
    {
        return new StartClient( System.out, System.err )
        {
            @Override
            protected GraphDatabaseShellServer getGraphDatabaseShellServer( File path, boolean readOnly,
                    String configFile ) throws RemoteException
            {
                return new GraphDatabaseShellServer( db );
            }
        };
    }

    private long lastClosedTxId()
    {
        return db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();
    }

    private String createCsvFile( long size ) throws IOException
    {
        File tmpFile = File.createTempFile( "data", ".csv", null );
        tmpFile.deleteOnExit();
        try ( PrintWriter out = new PrintWriter( tmpFile ) )
        {
            out.println( "foo" );
            for ( int i = 0; i < size; i++ )
            {
                out.println( i );
            }
        }
        return tmpFile.toURI().toURL().toExternalForm();
    }

    private void createNodeAndLockItInDifferentThread( String label, String property, Object value ) throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label( label ) ).setProperty( property, value );
            tx.success();
        }

        runAsync( () ->
        {
            Transaction tx = db.beginTx();
            Node node = db.findNode( label( "Person" ), "id", 42L );
            assertNotNull( node );
            tx.acquireWriteLock( node );
        } ).get( 1, MINUTES );
    }

}
