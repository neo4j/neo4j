/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.index.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.kernel.apps.GraphDatabaseApp;

@Service.Implementation( App.class )
public class IndexProviderShellApp extends GraphDatabaseApp
{
    {
        addOptionDefinition( "g", new OptionDefinition( OptionValueType.NONE,
                "Get nodes for the given key and value" ) );
        addOptionDefinition( "q", new OptionDefinition( OptionValueType.NONE,
                "Get nodes for the given query" ) );
        addOptionDefinition( "i", new OptionDefinition( OptionValueType.NONE,
                "Index the current node with a key and (optionally) value. " +
                "If no value is given the property value for the key is " +
                "used" ) );
        addOptionDefinition( "r", new OptionDefinition( OptionValueType.NONE,
                "Removes a key-value pair for the current node from the index. " +
                "If no value is given the property value for the key is used" ) );
        addOptionDefinition( "c", OPTION_DEF_FOR_C );
        addOptionDefinition( "cd", new OptionDefinition( OptionValueType.NONE,
                "Does a 'cd' command to the returned node. " +
                "Could also be done using the -c option. (Implies -g)" ) );
        addOptionDefinition( "ls", new OptionDefinition( OptionValueType.NONE,
                "Does a 'ls' command on the returned nodes. " +
                "Could also be done using the -c option. (Implies -g)" ) );
    }

    @Override
    public String getName()
    {
        return "index";
    }

    @Override
    public String getDescription()
    {
        return "Access the IndexProvider capabilities for your Neo4j graph database. " +
        "Use -g for getting nodes, -i and -r to manipulate. Examples:\n" +
        "index -i persons name  (will index property 'name' with its value for current node in the persons index)\n" +
        "index -g persons name \"Thomas A. Anderson\"  (will get nodes matching that name from the persons index)\n" +
        "index -q persons \"name:'Thomas*'\"  (will get nodes with names that start with Thomas)\n" +
        "index --cd persons name \"Agent Smith\"  (will 'cd' to the 'Agent Smith' node from the persons index).";
    }

    @Override
    protected String exec( AppCommandParser parser, Session session, Output out )
            throws ShellException, RemoteException
    {
        boolean doCd = parser.options().containsKey( "cd" );
        boolean doLs = parser.options().containsKey( "ls" );
        boolean query = parser.options().containsKey( "q" );
        boolean get = parser.options().containsKey( "g" ) || query || doCd || doLs;
        boolean index = parser.options().containsKey( "i" );
        boolean remove = parser.options().containsKey( "r" );
        int count = boolCount( get, index, remove );
        if ( count != 1 )
        {
            throw new ShellException( "Supply one of: -g, -i, -r" );
        }

        GraphDatabaseService db = getServer().getDb();
        if ( get )
        {
            String commandToRun = parser.options().get( "c" );
            Collection<String> commandsToRun = new ArrayList<String>();
            boolean specialCommand = false;
            if ( doCd || doLs )
            {
                specialCommand = true;
                if ( doCd )
                {
                    commandsToRun.add( "cd -a $n" );
                }
                else if ( doLs )
                {
                    commandsToRun.add( "ls $n" );
                }
            }
            else if ( commandToRun != null )
            {
                commandsToRun.addAll( Arrays.asList( commandToRun.split( Pattern.quote( "&&" ) ) ) );
            }
            
            String indexName = parser.arguments().get( 0 );
            if ( !db.index().existsForNodes( indexName ) )
            {
                out.println( "No such index '" + indexName + "'" );
                return null;
            }
            
            Iterable<Node> result = query ? query( db, parser ) : get( db, parser );
            for ( Node node : result )
            {
                printAndInterpretTemplateLines( commandsToRun, false, !specialCommand, node,
                        getServer(), session, out );
            }
        }
        else if ( index )
        {
            index( db, parser, session );
        }
        else if ( remove )
        {
            remove( db, parser, session );
        }
        return null;
    }

    private int boolCount( boolean... bools )
    {
        int count = 0;
        for ( boolean bool : bools )
        {
            if ( bool )
            {
                count++;
            }
        }
        return count;
    }

    private Iterable<Node> get( GraphDatabaseService db, AppCommandParser parser )
    {
        String index = parser.arguments().get( 0 );
        String key = parser.arguments().get( 1 );
        String value = parser.arguments().get( 2 );
        return db.index().forNodes( index ).get( key, value );
    }

    private Iterable<Node> query( GraphDatabaseService db, AppCommandParser parser )
    {
        String index = parser.arguments().get( 0 );
        String query = parser.arguments().get( 1 );
        return db.index().forNodes( index ).query( query );
    }

    private void index( GraphDatabaseService db, AppCommandParser parser, Session session )
            throws ShellException
    {
        Node node = getCurrent( session ).asNode();
        String index = parser.arguments().get( 0 );
        String key = parser.arguments().get( 1 );
        Object value = parser.arguments().size() > 2 ? parser.arguments().get( 2 )
                : node.getProperty( key, null );
        if ( value == null )
        {
            throw new ShellException( "No value to index" );
        }
        db.index().forNodes( index ).add( node, key, value );
    }

    private void remove( GraphDatabaseService db, AppCommandParser parser, Session session )
            throws ShellException
    {
        Node node = getCurrent( session ).asNode();
        String index = parser.arguments().get( 0 );
        String key = parser.arguments().get( 0 );
        Object value = parser.arguments().size() > 1 ? parser.arguments().get( 1 )
                : node.getProperty( key, null );
        if ( value == null )
        {
            throw new ShellException( "No value to remove" );
        }
        db.index().forNodes( index ).remove( node, key, value );
    }
}
