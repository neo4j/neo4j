/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.Test;

import org.neo4j.cypher.NodeStillHasRelationshipsException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.shell.impl.CollectingOutput;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;
import static org.neo4j.helpers.collection.MapUtil.genericMap;

public class TestApps extends AbstractShellTest
{
    @Test
    public void variationsOfCdAndPws() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 3 );
        executeCommand( "cd" );
        executeCommand( "pwd", pwdOutputFor( relationships[0].getStartNode() ) );
        executeCommandExpectingException( "cd " + relationships[0].getStartNode().getId(), "stand" );
        executeCommand( "pwd", pwdOutputFor( relationships[0].getStartNode() ) );
        executeCommand( "cd " + relationships[0].getEndNode().getId() );
        executeCommand( "pwd", pwdOutputFor( relationships[0].getStartNode(), relationships[0].getEndNode() ) );
        executeCommandExpectingException( "cd " + relationships[2].getEndNode().getId(), "connected" );
        executeCommand( "pwd", pwdOutputFor( relationships[0].getStartNode(), relationships[0].getEndNode() ) );
        executeCommand( "cd -a " + relationships[2].getEndNode().getId() );
        executeCommand( "pwd", pwdOutputFor( relationships[0].getStartNode(), relationships[0].getEndNode(), relationships[2].getEndNode() ) );
        executeCommand( "cd .." );
        executeCommand( "pwd", pwdOutputFor( relationships[0].getStartNode(), relationships[0].getEndNode() ) );
        executeCommand( "cd " + relationships[1].getEndNode().getId() );
        executeCommand( "pwd", pwdOutputFor( relationships[0].getStartNode(), relationships[0].getEndNode(), relationships[1].getEndNode() ) );
    }

    @Test
    public void canSetPropertiesAndLsWithFilters() throws Exception
    {
        RelationshipType type1 = DynamicRelationshipType.withName( "KNOWS" );
        RelationshipType type2 = DynamicRelationshipType.withName( "LOVES" );
        Relationship[] relationships = createRelationshipChain( type1, 2 );
        Node node = relationships[0].getEndNode();
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
        Node node = relationships[0].getEndNode();
        executeCommand( "cd " + node.getId() );
        String name = "Mattias";
        executeCommand( "set name " + name );
        int age = 31;
        executeCommand( "set age -t int " + age );
        executeCommand( "set \"some property\" -t long[] \"[1234,5678]" );
        assertEquals( name, node.getProperty( "name" ) );
        assertEquals( age, node.getProperty( "age" ) );
        long[] value = (long[]) node.getProperty( "some property" );
        assertTrue( Arrays.equals( new long[]{1234L, 5678L}, value ) );

        executeCommand( "rm age" );
        assertNull( node.getProperty( "age", null ) );
        assertEquals( name, node.getProperty( "name" ) );
    }

    @Test
    public void canCreateRelationshipsAndNodes() throws Exception
    {
        RelationshipType type1 = withName( "type1" );
        RelationshipType type2 = withName( "type2" );
        RelationshipType type3 = withName( "type3" );

        // No type supplied
        executeCommandExpectingException( "mkrel -c", "type" );

        executeCommand( "mkrel -ct " + type1.name() );
        Relationship relationship = db.getReferenceNode().getSingleRelationship( type1, Direction.OUTGOING );
        Node node = relationship.getEndNode();
        executeCommand( "mkrel -t " + type2.name() + " " + node.getId() );
        Relationship otherRelationship = db.getReferenceNode().getSingleRelationship( type2, Direction.OUTGOING );
        assertEquals( node, otherRelationship.getEndNode() );

        // With properties
        executeCommand( "mkrel -ct " + type3.name() + " --np \"{'name':'Neo','destiny':'The one'}\" --rp \"{'number':11}\"" );
        Relationship thirdRelationship = db.getReferenceNode().getSingleRelationship( type3, Direction.OUTGOING );
        assertEquals( 11, thirdRelationship.getProperty( "number" ) );
        Node thirdNode = thirdRelationship.getEndNode();
        assertEquals( "Neo", thirdNode.getProperty( "name" ) );
        assertEquals( "The one", thirdNode.getProperty( "destiny" ) );
        executeCommand( "cd -r " + thirdRelationship.getId() );
        executeCommand( "mv number other-number" );
        assertNull( thirdRelationship.getProperty( "number", null ) );
        assertEquals( 11, thirdRelationship.getProperty( "other-number" ) );
        
        // Create and go to
        executeCommand( "cd end" );
        executeCommand( "mkrel -ct " + type1.name() + " --np \"{'name':'new'}\" --cd" );
        executeCommand( "ls -p", "name", "new" );
    }

    @Test
    public void rmrelCanLeaveStrandedIslands() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 4 );
        executeCommand( "cd -a " + relationships[1].getEndNode().getId() );

        Relationship relToDelete = relationships[2];
        executeCommandExpectingException( "rmrel " + relToDelete.getId(), "decoupled" );
        assertRelationshipExists( relToDelete );

        Node otherNode = relToDelete.getEndNode();
        executeCommand( "rmrel -fd " + relToDelete.getId() );
        assertRelationshipDoesntExist( relToDelete );
        assertNodeExists( otherNode );
    }

    @Test
    public void rmrelCanLeaveStrandedNodes() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 1 );
        Node otherNode = relationships[0].getEndNode();

        executeCommandExpectingException( "rmrel " + relationships[0].getId(), "decoupled" );
        assertRelationshipExists( relationships[0] );
        assertNodeExists( otherNode );

        executeCommand( "rmrel -f " + relationships[0].getId() );
        assertRelationshipDoesntExist( relationships[0] );
        assertNodeExists( otherNode );
    }

    @Test
    public void rmrelCanDeleteStrandedNodes() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 1 );
        Node otherNode = relationships[0].getEndNode();

        executeCommand( "rmrel -fd " + relationships[0].getId(), "not having any relationships" );
        assertRelationshipDoesntExist( relationships[0] );
        assertNodeDoesntExist( otherNode );
    }

    @Test
    public void rmrelCanDeleteRelationshipSoThatCurrentNodeGetsStranded() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 2 );
        executeCommand( "cd " + relationships[0].getEndNode().getId() );
        deleteRelationship( relationships[0] );
        Node currentNode = relationships[1].getStartNode();
        executeCommand( "rmrel -fd " + relationships[1].getId(), "not having any relationships" );
        assertNodeExists( currentNode );
        assertFalse( currentNode.hasRelationship() );
        executeCommand( "pwd" );
        executeCommand( "cd -a " + db.getReferenceNode().getId() );
        executeCommand( "pwd" );
    }

    @Test
    public void rmnodeCanDeleteStrandedNodes() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 1 );
        Node strandedNode = relationships[0].getEndNode();
        deleteRelationship( relationships[0] );
        executeCommand( "rmnode " + strandedNode.getId() );
        assertNodeDoesntExist( strandedNode );
    }

    @Test
    public void rmnodeCanDeleteConnectedNodes() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 2 );
        Node middleNode = relationships[0].getEndNode();
        executeCommandExpectingException( "rmnode " + middleNode.getId(), "still has relationships" );
        assertNodeExists( middleNode );
        Node endNode = relationships[1].getEndNode();
        executeCommand( "rmnode -f " + middleNode.getId(), "deleted" );
        assertNodeDoesntExist( middleNode );
        assertRelationshipDoesntExist( relationships[0] );
        assertRelationshipDoesntExist( relationships[1] );

        assertNodeExists( endNode );
        executeCommand( "cd -a " + endNode.getId() );
        executeCommand( "rmnode " + endNode.getId() );
        executeCommand( "pwd", Pattern.quote( "(?)" ) );
    }

    @Test
    public void pwdWorksOnDeletedNode() throws Exception
    {
        Relationship[] relationships = createRelationshipChain( 1 );
        executeCommand( "cd " + relationships[0].getEndNode().getId() );

        // Delete the relationship and node we're standing on
        beginTx();
        relationships[0].getEndNode().delete();
        relationships[0].delete();
        finishTx();

        Relationship[] otherRelationships = createRelationshipChain( 1 );
        executeCommand( "pwd", "\\(0\\)-->\\(\\?\\)" );
        executeCommand( "cd -a " + otherRelationships[0].getEndNode().getId() );
        executeCommand( "ls" );
    }

    @Test
    public void startEvenIfReferenceNodeHasBeenDeleted() throws Exception
    {
        Transaction tx = db.beginTx();
        db.getReferenceNode().delete();
        Node node = db.createNode();
        String name = "Test";
        node.setProperty( "name", name );
        tx.success();
        tx.finish();

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
        executeCommand( "START n = node({self}) RETURN n.name;", nodeOneName );
        executeCommand( "cd -r " + relationship.getId() );
        executeCommand( "START r = relationship({self}) RETURN r.name;", relationshipName );
        executeCommand( "cd " + otherNode.getId() );
        executeCommand( "START n = node({self}) RETURN n.name;", nodeTwoName );
        
        executeCommand( "cd -a " + strayNode.getId() );
        beginTx();
        strayNode.delete();
        finishTx();
        executeCommand( "START n = node(" + node.getId() + ") RETURN n.name;", nodeOneName );
    }

    @Test
    public void cypherTiming() throws Exception
    {
        beginTx();
        Node node = db.createNode();
        Node otherNode = db.createNode();
        node.createRelationshipTo( otherNode, RELATIONSHIP_TYPE );
        finishTx();

        executeCommand( "START n = node(" + node.getId() + ") match p=n-[r?*]-m RETURN p;", "\\d+ ms" );
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
        assertTrue( Arrays.equals( new int[] {1,2,3,4}, (int[]) getCurrentNode().getProperty( "values" ) ) );
    }
    
    @Test
    public void createNodeWithLabel() throws Exception
    {
        executeCommand( "mknode --cd -l Person" );
        assertEquals( asSet( "Person" ), asSet( names( getCurrentNode().getLabels() ) ) );
    }
    
    @Test
    public void createNodeWithColonPrefixedLabel() throws Exception
    {
        executeCommand( "mknode --cd -l :Person" );
        assertEquals( asSet( "Person" ), asSet( names( getCurrentNode().getLabels() ) ) );
    }
    
    @Test
    public void createNodeWithPropertiesAndLabels() throws Exception
    {
        executeCommand( "mknode --cd --np \"{'name': 'Test'}\" -l \"['Person', ':Thing']\"" );
        assertEquals( "Test", getCurrentNode().getProperty( "name" ) );
        assertEquals( asSet( "Person", "Thing" ), asSet( names( getCurrentNode().getLabels() ) ) );
    }
    
    @Test
    public void createRelationshipWithArrayProperty() throws Exception
    {
        String type = "ARRAY";
        executeCommand( "mkrel -ct " + type + " --rp \"{'values':[1,2,3,4]}\"" );
        assertTrue( Arrays.equals( new int[] {1,2,3,4},
                (int[]) getCurrentNode().getSingleRelationship( withName( type ), OUTGOING ).getProperty( "values" ) ) );
    }
    
    @Test
    public void createRelationshipToNewNodeWithLabels() throws Exception
    {
        String type = "TEST";
        executeCommand( "mkrel -ctl " + type + " Person" );
        assertEquals( asSet( "Person" ), asSet( names( getCurrentNode().getSingleRelationship(
                withName( type ), OUTGOING ).getEndNode().getLabels() ) ) );
    }
    
    @Test
    public void getDbinfo() throws Exception
    {
        // It's JSON coming back from dbinfo command
        executeCommand( "dbinfo -g Kernel", "\\{", "\\}", "StoreId" );
    }
    
    @Test
    public void evalOneLinerExecutesImmediately() throws Exception
    {
        executeCommand( "eval db.createNode()", "Node\\[" );
    }
    
    @Test
    public void evalMultiLineExecutesAfterAllLines() throws Exception
    {
        executeCommand(
                "eval\n" +
                "node = db.createNode()\n" +
                "node.setProperty( \"name\", \"Mattias\" )\n" +
                "node.getProperty( \"name\" )\n", "Mattias" );
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
        executeCommand( "env", "a=true", "b=100", "c=\"foo\"" );
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
        executeCommand( "x", "Current is \\(0\\)" );
    }

    @Test
    public void cypherNodeStillHasRelationshipsException() throws Exception
    {
        try
        {
            executeCommand("create a,b,a-[:x]->b;");
            executeCommand("start n=node(*) delete n;");
            fail( "Should have failed with " + NodeStillHasRelationshipsException.class.getName() + " exception" );
        }
        catch ( ShellException e )
        {
            assertTrue( "Expected notice about cause not found in " + e.getMessage(),
                    e.getMessage().contains( NodeStillHasRelationshipsException.class.getSimpleName() ) );
        }
    }

    @Test
    public void use_cypher_merge() throws Exception
    {
        executeCommand( "merge (n:Person {name:'Andres'});" );
        ResourceIterable<Node> iter = db.findNodesByLabelAndProperty( label( "Person" ), "name", "Andres" );
        ResourceIterator<Node> iterator = iter.iterator();


        assertTrue("Did not find expected node", iterator.hasNext());
        iterator.next();
        assertFalse("MERGE seems to have created multiple nodes", iterator.hasNext());
    }

    @Test
    public void canSetInitialSessionVariables() throws Exception
    {
        Map<String, Serializable> values = genericMap( "mykey", "myvalue",
                                                       "my_other_key", "My other value" );
        ShellClient client = newShellClient( shellServer, values );
        String[] allStrings = new String[values.size()*2];
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
        assertEquals( "Shows welcome message: " + outString, false, outString.contains( "Welcome to the Neo4j Shell! Enter 'help' for a list of commands" ) );
    }

    @Test
    public void doesShowWelcomeMessage() throws Exception
    {
        Map<String, Serializable> values = genericMap();
        final CollectingOutput out = new CollectingOutput();
        ShellClient client = new SameJvmClient( values, shellServer, out );
        client.shutdown();
        final String outString = out.asString();
        assertEquals( "Shows welcome message: " + outString, true, outString.contains( "Welcome to the Neo4j Shell! Enter 'help' for a list of commands" ) );
    }

    @Test
    public void canExecuteCypherWithShellVariables() throws Exception
    {
        Map<String, Serializable> variables = genericMap( "id", 0 );
        ShellClient client = newShellClient( shellServer, variables );
        executeCommand( client, "start n=node({id}) return n;", "1 row" );
    }

    @Test
    public void canDumpSubgraphWithCypher() throws Exception
    {
        final DynamicRelationshipType type = DynamicRelationshipType.withName( "KNOWS" );
        createRelationshipChain( db.getReferenceNode(), type, 1 );
        executeCommand( "dump start n=node(0) match n-[r]->m return n,r,m;",
                "begin",
                "start _0 = node\\(0\\) with _0 ",
                "create \\(_1\\)",
                "_0-\\[:`KNOWS`\\]->_1",
                "commit" );
    }

    @Test
    public void canDumpGraph() throws Exception
    {
        final DynamicRelationshipType type = DynamicRelationshipType.withName( "KNOWS" );
        final Relationship rel = createRelationshipChain( db.getReferenceNode(), type, 1 )[0];
        db.beginTx();
        rel.getStartNode().setProperty( "f o o", "bar" );
        rel.setProperty( "since", 2010 );
        rel.getEndNode().setProperty( "flags", new Boolean[]{true, false, true} );
        executeCommand( "dump ",
                "begin",
                "start _0 = node\\(0\\) with _0 ",
                "set _0.`f o o`=\"bar\"",
                "create \\(_1 \\{`flags`:\\[true, false, true\\]\\}\\)",
                "_0-\\[:`KNOWS` \\{`since`:2010\\}\\]->_1",
                "commit"
        );
    }

    @Test
    public void commentsAreIgnored() throws Exception
    {
        executeCommand(
                "eval\n" +
                "// This comment should be ignored\n" +
                "node = db.createNode()\n" +
                "node.setProperty( \"name\", \"Mattias\" )\n" +
                "node.getProperty( \"name\" )\n", "Mattias" );
    }
    
    @Test
    public void canAddLabelToNode() throws Exception
    {
        // GIVEN
        Relationship[] chain = createRelationshipChain( 1 );
        Node node = chain[0].getEndNode();
        executeCommand( "cd -a " + node.getId() );
        
        // WHEN
        executeCommand( "set -l Person" );
        
        // THEN
        assertEquals( asSet( "Person" ), asSet( names( node.getLabels() ) ) );
    }
    
    @Test
    public void canAddMultipleLabelsToNode() throws Exception
    {
        // GIVEN
        Relationship[] chain = createRelationshipChain( 1 );
        Node node = chain[0].getEndNode();
        executeCommand( "cd -a " + node.getId() );
        
        // WHEN
        executeCommand( "set -l ['Person','Thing']" );
        
        // THEN
        assertEquals( asSet( "Person", "Thing" ), asSet( names( node.getLabels() ) ) );
    }
    
    @Test
    public void canRemoveLabelFromNode() throws Exception
    {
        // GIVEN
        beginTx();
        Relationship[] chain = createRelationshipChain( 1 );
        Node node = chain[0].getEndNode();
        node.addLabel( label( "Person" ) );
        finishTx();
        executeCommand( "cd -a " + node.getId() );

        // WHEN
        executeCommand( "rm -l Person" );

        // THEN
        assertEquals( emptySetOf( String.class ), asSet( names( node.getLabels() ) ) );
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
        assertEquals( asSet( "Thing" ), asSet( names( node.getLabels() ) ) );
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
        db.schema().awaitIndexOnline( index, 10, SECONDS );

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
        db.schema().awaitIndexOnline( index1, 10, SECONDS );
        db.schema().awaitIndexOnline( index2, 10, SECONDS );

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
        db.schema().awaitIndexOnline( index1, 10, SECONDS );
        db.schema().awaitIndexOnline( index2, 10, SECONDS );
        db.schema().awaitIndexOnline( index3, 10, SECONDS );
        db.schema().awaitIndexOnline( index4, 10, SECONDS );

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
        assertEquals( IndexState.ONLINE, db.schema().getIndexState( index ) );
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
        db.schema().awaitIndexOnline( index, 10, SECONDS );

        // WHEN / THEN
        executeCommand( "schema", label.name(), property );
    }

    @Test
    public void canListConstraints() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        beginTx();
        db.schema().constraintFor( label ).unique().on( "name" ).create();
        finishTx();

        // WHEN / THEN
        executeCommand( "schema ls", "ON \\(person:Person\\) ASSERT person.name IS UNIQUE" );
    }

    @Test
    public void canListConstraintsByLabel() throws Exception
    {
        // GIVEN
        Label label1 = label( "Person" );
        beginTx();
        db.schema().constraintFor( label1 ).unique().on( "name" ).create();
        finishTx();

        // WHEN / THEN
        executeCommand( "schema ls -l :Person", "ON \\(person:Person\\) ASSERT person.name IS UNIQUE");
    }

    @Test
    public void canListConstraintsByLabelAndProperty() throws Exception
    {
        // GIVEN
        Label label1 = label( "Person" );
        beginTx();
        db.schema().constraintFor( label1 ).unique().on( "name" ).create();
        finishTx();

        // WHEN / THEN
        executeCommand( "schema ls -l :Person -p name", "ON \\(person:Person\\) ASSERT person.name IS UNIQUE" );
    }

    private Iterable<String> names( Iterable<Label> labels )
    {
        return Iterables.map( new Function<Label, String>()
        {
            @Override
            public String apply( Label label )
            {
                return label.name();
            }
        }, labels );
    }
}
