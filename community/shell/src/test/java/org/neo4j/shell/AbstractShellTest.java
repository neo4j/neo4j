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

import static java.lang.Integer.parseInt;
import static java.util.regex.Pattern.compile;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.impl.AbstractServer;
import org.neo4j.shell.impl.CollectingOutput;
import org.neo4j.shell.impl.RmiLocation;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.kernel.apps.GraphDatabaseApp;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

@Ignore( "Not a unit test" )
public abstract class AbstractShellTest
{
    protected GraphDatabaseAPI db;
    protected ShellServer shellServer;
    private ShellClient shellClient;
    private Integer remotelyAvailableOnPort;
    protected static final RelationshipType RELATIONSHIP_TYPE = withName( "TYPE" );
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    private Transaction tx;

    @Before
    public void doBefore() throws Exception
    {
        db = newDb();
        shellServer = newServer( db );
        shellClient = ShellLobby.newClient( shellServer );
    }
    
    protected GraphDatabaseAPI newDb()
    {
        return (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( fs.get() ).newImpermanentDatabase();
    }

    protected ShellServer newServer( GraphDatabaseAPI db ) throws ShellException, RemoteException
    {
        return new GraphDatabaseShellServer( db );
    }

    @After
    public void doAfter() throws Exception
    {
        shellClient.shutdown();
        shellServer.shutdown();
        db.shutdown();
    }

    protected void beginTx()
    {
        assert tx == null;
        tx = db.beginTx();
    }
    
    protected void finishTx()
    {
        finishTx( true );
    }
    
    protected ShellClient newRemoteClient() throws Exception
    {
        return newRemoteClient( new HashMap<String, Serializable>() );
    }
    
    protected ShellClient newRemoteClient( Map<String, Serializable> initialSession ) throws Exception
    {
        return ShellLobby.newClient( RmiLocation.location( "localhost",
                remotelyAvailableOnPort.intValue(), AbstractServer.DEFAULT_NAME ), initialSession );
    }

    protected void makeServerRemotelyAvailable() throws RemoteException
    {
        if ( remotelyAvailableOnPort == null )
        {
            remotelyAvailableOnPort = findFreePort();
            shellServer.makeRemotelyAvailable( remotelyAvailableOnPort.intValue(), AbstractServer.DEFAULT_NAME );
        }
    }
    
    private int findFreePort()
    {
        // TODO
        return AbstractServer.DEFAULT_PORT;
    }

    protected void restartServer() throws Exception
    {
        shellServer.shutdown();
        db.shutdown();
        db = newDb();
        remotelyAvailableOnPort = null;
        shellServer = newServer( db );
        shellClient = ShellLobby.newClient( shellServer );
    }

    protected void finishTx( boolean success )
    {
        assert tx != null;
        if ( success )
            tx.success();
        tx.finish();
        tx = null;
    }

    protected static String pwdOutputFor( Object... entities )
    {
        StringBuilder builder = new StringBuilder();
        for ( Object entity : entities )
        {
            builder.append( (builder.length() == 0 ? "" : "-->") );
            if ( entity instanceof Node )
            {
                builder.append( "(" + ((Node)entity).getId() + ")" );
            }
            else
            {
                builder.append( "<" + ((Relationship)entity).getId() + ">" );
            }
        }
        return Pattern.quote( builder.toString() );
    }

    public void executeCommand( String command, String... theseLinesMustExistRegEx ) throws Exception
    {
        executeCommand( shellClient, command, theseLinesMustExistRegEx );
    }

    public void executeCommand( ShellClient client, String command,
            String... theseLinesMustExistRegEx ) throws Exception
    {
        CollectingOutput output = new CollectingOutput();
        client.evaluate( command, output );

        for ( String lineThatMustExist : theseLinesMustExistRegEx )
        {
            boolean negative = lineThatMustExist.startsWith( "!" );
            lineThatMustExist = negative ? lineThatMustExist.substring( 1 ) : lineThatMustExist;
            Pattern pattern = compile( lineThatMustExist );
            boolean found = false;
            for ( String line : output )
            {
                if ( pattern.matcher( line ).find() )
                {
                    found = true;
                    break;
                }
            }
            assertTrue( "Was expecting a line matching '" + lineThatMustExist + "', but didn't find any from out of " +
                    asCollection( output ), found != negative );
        }
    }

    public void executeCommandExpectingException( String command, String errorMessageShouldContain ) throws Exception
    {
        CollectingOutput output = new CollectingOutput();
        try
        {
            shellClient.evaluate( command, output );
            fail( "Was expecting an exception" );
        }
        catch ( ShellException e )
        {
            String errorMessage = e.getMessage();
            if ( !errorMessage.toLowerCase().contains( errorMessageShouldContain.toLowerCase() ) )
            {
                fail( "Error message '" + errorMessage + "' should have contained '" + errorMessageShouldContain + "'" );
            }
        }
    }

    protected void assertRelationshipExists( Relationship relationship )
    {
        assertRelationshipExists( relationship.getId() );
    }

    protected void assertRelationshipExists( long id )
    {
        try
        {
            db.getRelationshipById( id );
        }
        catch ( NotFoundException e )
        {
            fail( "Relationship " + id + " should exist" );
        }
    }

    protected void assertRelationshipDoesntExist( Relationship relationship )
    {
        assertRelationshipDoesntExist( relationship.getId() );
    }

    protected void assertRelationshipDoesntExist( long id )
    {
        try
        {
            db.getRelationshipById( id );
            fail( "Relationship " + id + " shouldn't exist" );
        }
        catch ( NotFoundException e )
        { // Good
        }
    }

    protected void assertNodeExists( Node node )
    {
        assertNodeExists( node.getId() );
    }

    protected void assertNodeExists( long id )
    {
        try
        {
            db.getNodeById( id );
        }
        catch ( NotFoundException e )
        {
            fail( "Node " + id + " should exist" );
        }
    }

    protected void assertNodeDoesntExist( Node node )
    {
        assertNodeDoesntExist( node.getId() );
    }

    protected void assertNodeDoesntExist( long id )
    {
        try
        {
            db.getNodeById( id );
            fail( "Relationship " + id + " shouldn't exist" );
        }
        catch ( NotFoundException e )
        { // Good
        }
    }

    protected Relationship[] createRelationshipChain( int length )
    {
        return createRelationshipChain( RELATIONSHIP_TYPE, length );
    }

    protected Relationship[] createRelationshipChain( RelationshipType type, int length )
    {
        return createRelationshipChain( db.getReferenceNode(), type, length );
    }

    protected Relationship[] createRelationshipChain( Node startingFromNode, RelationshipType type,
            int length )
    {
        Relationship[] rels = new Relationship[length];
        Transaction tx = db.beginTx();
        Node firstNode = startingFromNode;
        for ( int i = 0; i < rels.length; i++ )
        {
            Node secondNode = db.createNode();
            rels[i] = firstNode.createRelationshipTo( secondNode, type );
            firstNode = secondNode;
        }
        tx.success();
        tx.finish();
        return rels;
    }

    protected void deleteRelationship( Relationship relationship )
    {
        Transaction tx = db.beginTx();
        relationship.delete();
        tx.success();
        tx.finish();
    }

    protected void setProperty( Node node, String key, Object value )
    {
        Transaction tx = db.beginTx();
        node.setProperty( key, value );
        tx.success();
        tx.finish();
    }
    
    protected Node getCurrentNode() throws RemoteException, ShellException
    {
        Serializable current = shellServer.interpretVariable( shellClient.getId(), GraphDatabaseApp.CURRENT_KEY );
        return this.db.getNodeById( parseInt( current.toString().substring( 1 ) ) );
    }
}
