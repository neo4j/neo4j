/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static java.util.regex.Pattern.compile;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.shell.impl.RemoteOutput;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

@Ignore
public abstract class AbstractShellTest
{
    private static final String DB_PATH = "target/var/shelldb";
    protected static GraphDatabaseService db;
    private static ShellServer shellServer;
    private ShellClient shellClient;
    protected static final RelationshipType RELATIONSHIP_TYPE = withName( "TYPE" );
    
    @BeforeClass
    public static void startUp() throws Exception
    {
        deleteRecursively( new File( DB_PATH ) );
        db = new EmbeddedGraphDatabase( DB_PATH );
        shellServer = new GraphDatabaseShellServer( db );
    }
    
    @Before
    public void doBefore()
    {
        clearDb();
        shellClient = new SameJvmClient( shellServer );
    }
    
    private void clearDb()
    {
        Transaction tx = db.beginTx();
        for ( Node node : db.getAllNodes() )
        {
            for ( Relationship rel : node.getRelationships( Direction.OUTGOING ) )
            {
                rel.delete();
            }
            if ( !node.equals( db.getReferenceNode() ) )
            {
                node.delete();
            }
        }
        tx.success();
        tx.finish();
    }

    @After
    public void doAfter()
    {
        shellClient.shutdown();
    }
    
    @AfterClass
    public static void shutDown() throws Exception
    {
        shellServer.shutdown();
        db.shutdown();
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
        executeCommand( shellServer, shellClient, command, theseLinesMustExistRegEx );
    }
    
    public void executeCommand( ShellServer server, ShellClient client, String command,
            String... theseLinesMustExistRegEx ) throws Exception
    {
        OutputCollector output = new OutputCollector();
        server.interpretLine( command, client.session(), output );
        
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
        OutputCollector output = new OutputCollector();
        try
        {
            shellServer.interpretLine( command, shellClient.session(), output );
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

    public class OutputCollector implements Output, Serializable, Iterable<String>
    {
        private static final long serialVersionUID = 1L;
        private final List<String> lines = new ArrayList<String>();
        private String ongoingLine = "";

        @Override
        public Appendable append( CharSequence csq, int start, int end )
                throws IOException
        {
            this.print( RemoteOutput.asString( csq ).substring( start, end ) );
            return this;
        }

        @Override
        public Appendable append( char c ) throws IOException
        {
            this.print( c );
            return this;
        }

        @Override
        public Appendable append( CharSequence csq ) throws IOException
        {
            this.print( RemoteOutput.asString( csq ) );
            return this;
        }

        @Override
        public void println( Serializable object ) throws RemoteException
        {
            print( object );
            println();
        }

        @Override
        public void println() throws RemoteException
        {
            lines.add( ongoingLine );
            ongoingLine = "";
        }

        @Override
        public void print( Serializable object ) throws RemoteException
        {
            ongoingLine += object.toString();
        }

        @Override
        public Iterator<String> iterator()
        {
            return lines.iterator();
        }
    }
}
