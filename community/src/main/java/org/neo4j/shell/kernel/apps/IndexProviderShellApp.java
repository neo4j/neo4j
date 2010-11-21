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

package org.neo4j.shell.kernel.apps;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

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
        addOptionDefinition( "create", new OptionDefinition( OptionValueType.NONE,
                "Creates a new index with a set of configuration parameters" ) );
        addOptionDefinition( "get-config", new OptionDefinition( OptionValueType.NONE,
                "Displays the configuration for an index" ) );
        addOptionDefinition( "set-config", new OptionDefinition( OptionValueType.NONE,
                "EXPERT, USE WITH CARE: Set one configuration parameter for an index (remove if no value)" ) );
        addOptionDefinition( "t", new OptionDefinition( OptionValueType.MUST,
                "The type of index, either Node or Relationship" ) );
        addOptionDefinition( "indexes", new OptionDefinition( OptionValueType.NONE, "Lists all index names" ) );
        addOptionDefinition( "delete", new OptionDefinition( OptionValueType.NONE, "Deletes an index" ) );
    }

    @Override
    public String getName()
    {
        return "index";
    }

    @Override
    public String getDescription()
    {
        return "Access the integrated index for your Neo4j graph database. " +
        "Use -g for getting nodes, -i and -r to manipulate.\nExamples:\n" +
        "$ index -i persons name  (will index property 'name' with its value for current node in the 'persons' index)\n" +
        "$ index -g persons name \"Thomas A. Anderson\"  (will get nodes matching that name from the 'persons' index)\n" +
        "$ index -q persons \"name:'Thomas*'\"  (will get nodes with names that start with Thomas)\n" +
        "$ index --cd persons name \"Agent Smith\"  (will 'cd' to the 'Agent Smith' node from the 'persons' index).\n\n" +
        "EXPERT, USE WITH CARE. NOTE THAT INDEX DATA MAY BECOME INVALID AFTER CONFIGURATION CHANGES:\n" +
        "$ index --set-config accounts type fulltext  (will set parameter 'type'='fulltext' for 'accounts' index).\n" +
        "$ index --set-config accounts to_lower_case  (will remove parameter 'to_lower_case' from 'accounts' index).\n" +
        "$ index -t Relationship --delete friends  (will delete the 'friends' relationship index).";
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
        boolean getConfig = parser.options().containsKey( "get-config" );
        boolean create = parser.options().containsKey( "create" );
        boolean setConfig = parser.options().containsKey( "set-config" );
        boolean delete = parser.options().containsKey( "delete" );
        boolean indexes = parser.options().containsKey( "indexes" );
        int count = boolCount( get, index, remove, getConfig, create, setConfig, delete, indexes );
        if ( count != 1 )
        {
            throw new ShellException( "Supply one of: -g, -i, -r, --get-config, --set-config, --create, --delete, --indexes" );
        }

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
            
            if ( getIndex( parser.arguments().get( 0 ), Node.class, out ) == null )
            {
                return null;
            }
            
            Iterable<Node> result = query ? query( parser ) : get( parser );
            for ( Node node : result )
            {
                printAndInterpretTemplateLines( commandsToRun, false, !specialCommand, node,
                        getServer(), session, out );
            }
        }
        else if ( index )
        {
            index( parser, session );
        }
        else if ( remove )
        {
            if ( getIndex( parser.arguments().get( 0 ), Node.class, out ) == null )
            {
                return null;
            }
            remove( parser, session );
        }
        else if ( getConfig )
        {
            displayConfig( parser, out );
        }
        else if ( create )
        {
            createIndex( parser, out );
        }
        else if ( setConfig )
        {
            setConfig( parser, out );
        }
        else if ( delete )
        {
            deleteIndex( parser, out );
        }
        
        if ( indexes )
        {
            listIndexes( out );
        }
        
        return null;
    }
    
    private void listIndexes( Output out ) throws RemoteException
    {
        out.println( "Node indexes:" );
        for ( String name : getServer().getDb().index().nodeIndexNames() )
        {
            out.println( "  " + name );
        }
        out.println( "" );
        out.println( "Relationship indexes:" );
        for ( String name : getServer().getDb().index().relationshipIndexNames() )
        {
            out.println( "  " + name );
        }
    }

    private void deleteIndex( AppCommandParser parser, Output out ) throws RemoteException, ShellException
    {
        Index<? extends PropertyContainer> index = getIndex( parser.arguments().get( 0 ), getEntityType( parser ), out );
        if ( index != null )
        {
            index.delete();
        }
    }

    private void setConfig( AppCommandParser parser, Output out ) throws ShellException, RemoteException
    {
        String indexName = parser.arguments().get( 0 );
        String key = parser.arguments().get( 1 );
        String value = parser.arguments().size() > 2 ? parser.arguments().get( 2 ) : null;
        
        Class<? extends PropertyContainer> entityType = getEntityType( parser );
        Index<? extends PropertyContainer> index = getIndex( indexName, entityType, out );
        if ( index == null )
        {
            return;
        }
        String oldValue = value != null ? 
                getServer().getDb().index().setConfiguration( index, key, value ) :
                getServer().getDb().index().removeConfiguration( index, key );
        printWarning( out );
    }

    private void printWarning( Output out ) throws RemoteException
    {
        out.println( "INDEX CONFIGURATION CHANGED, INDEX DATA MAY BE INVALID" );
    }

    private void createIndex( AppCommandParser parser, Output out ) throws RemoteException, ShellException
    {
        String indexName = parser.arguments().get( 0 );
        Map config;
        try
        {
            config = parseJSONMap( parser.arguments().get( 1 ) );
        }
        catch ( JSONException e )
        {
            throw ShellException.wrapCause( e );
        }
        IndexStore indexStore = (IndexStore) ((AbstractGraphDatabase) getServer().getDb())
                .getConfig().getParams().get( IndexStore.class );
        Class<? extends PropertyContainer> entityType = getEntityType( parser );
        if ( indexStore.has( entityType, indexName ) )
        {
            out.println( entityType.getSimpleName() + " index '" + indexName + "' already exists" );
            return;
        }
        indexStore.set( entityType, indexName, config );
    }

    private <T extends PropertyContainer> Index<T> getIndex( String indexName, Class<T> type, Output out )
            throws RemoteException
    {
        IndexManager index = getServer().getDb().index();
        boolean exists = (type.equals( Node.class ) && index.existsForNodes( indexName )) ||
                (type.equals( Relationship.class ) && index.existsForRelationships( indexName ));
        if ( !exists )
        {
            out.println( "No such " + type.getSimpleName().toLowerCase() + " index '" + indexName + "'" );
            return null;
        }
        return (Index<T>) (type.equals( Node.class ) ? index.forNodes( indexName ) : index.forRelationships( indexName ));
    }
    
    private void displayConfig( AppCommandParser parser, Output out )
            throws RemoteException, ShellException
    {
        String indexName = parser.arguments().get( 0 );
        Index<? extends PropertyContainer> index = getIndex( indexName, getEntityType( parser ), out );
        if ( index == null )
        {
            return;
        }
        try
        {
            out.println( new JSONObject( getServer().getDb().index().getConfiguration( index ) ).toString( 4 ) );
        }
        catch ( JSONException e )
        {
            throw ShellException.wrapCause( e );
        }
    }

    private Class<? extends PropertyContainer> getEntityType( AppCommandParser parser ) throws ShellException
    {
        String type = parser.options().get( "t" );
        type = type != null ? type.toLowerCase() : null;
        if ( type == null || type.equals( "node" ) )
        {
            return Node.class;
        }
        else if ( type.equals( "relationship" ) )
        {
            return Relationship.class;
        }
        throw new ShellException( "'type' expects one of [Node, Relationship]" );
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

    private Iterable<Node> get( AppCommandParser parser )
    {
        String index = parser.arguments().get( 0 );
        String key = parser.arguments().get( 1 );
        String value = parser.arguments().get( 2 );
        return getServer().getDb().index().forNodes( index ).get( key, value );
    }

    private Iterable<Node> query( AppCommandParser parser )
    {
        String index = parser.arguments().get( 0 );
        String query1 = parser.arguments().get( 1 );
        String query2 = parser.arguments().size() > 2 ? parser.arguments().get( 2 ) : null;
        Index<Node> theIndex = getServer().getDb().index().forNodes( index );
        return query2 != null ? theIndex.query( query1, query2 ) : theIndex.query( query1 );
    }

    private void index( AppCommandParser parser, Session session ) throws ShellException
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
        getServer().getDb().index().forNodes( index ).add( node, key, value );
    }

    private void remove( AppCommandParser parser, Session session ) throws ShellException
    {
        Node node = getCurrent( session ).asNode();
        String index = parser.arguments().get( 0 );
        String key = parser.arguments().get( 1 );
        Object value = parser.arguments().size() > 2 ? parser.arguments().get( 2 )
                : node.getProperty( key, null );
        if ( value == null )
        {
            throw new ShellException( "No value to remove" );
        }
        getServer().getDb().index().forNodes( index ).remove( node, key, value );
    }
}
