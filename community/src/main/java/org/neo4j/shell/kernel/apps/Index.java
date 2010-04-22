package org.neo4j.shell.kernel.apps;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class Index extends GraphDatabaseApp
{
    public static final String KEY_INDEX_CLASS_NAME = "INDEX_CLASS_NAME";
    
    private Map<String, Object> indexServices = new HashMap<String, Object>();
    private boolean firstRun = true;
    
    {
        addOptionDefinition( "g", new OptionDefinition( OptionValueType.NONE,
                "Get nodes for the given key and value" ) );
        addOptionDefinition( "i", new OptionDefinition( OptionValueType.NONE,
                "Index the current node with a key and (optionally) value.\n" +
                "If no value is given the property value for the key is " +
                "used" ) );
        addOptionDefinition( "r", new OptionDefinition( OptionValueType.NONE,
                "Removes a key-value pair for the current node from the index.\n" +
                "If no value is given the property value for the key is used" ) );
        addOptionDefinition( "c", OPTION_DEF_FOR_C );
        addOptionDefinition( "cd", new OptionDefinition( OptionValueType.NONE,
                "Does a 'cd' command to the returned node.\n" +
                "Could also be done using the -c option. (Implies -g)" ) );
        addOptionDefinition( "ls", new OptionDefinition( OptionValueType.NONE,
                "Does a 'ls' command on the returned nodes.\n" +
                "Could also be done using the -c option. (Implies -g)" ) );
    }
    
    @Override
    public String getDescription()
    {
        return "Access the IndexService capabilities for your Neo4j graph database.\n" +
        		"Use -g for getting nodes, -i and -r to manipulate. Examples:\n" +
        		"index -i name  (will index property 'name' with its value for current node)\n" +
        		"index -g name \"Thomas A. Anderson\"  (will get nodes matching that name)\n" +
        		"index --cd name \"Agent Smith\"  (will 'cd' to the 'Agent Smith' node).";
    }
    
    @Override
    public void shutdown()
    {
        for ( Object indexService : this.indexServices.values() )
        {
            try
            {
                indexService.getClass().getMethod( "shutdown" ).invoke(
                        indexService );
            }
            catch ( Exception e )
            {
                // TODO OK?
                System.out.println( "Couldn't shut down index service " +
                        indexService + ", " + indexService.getClass() );
            }
        }
        this.indexServices.clear();
    }

    @Override
    protected String exec( AppCommandParser parser, Session session,
            Output out ) throws ShellException, RemoteException
    {
        if ( firstRun )
        {
            if ( safeGet( session, KEY_INDEX_CLASS_NAME ) == null )
            {
                safeSet( session, KEY_INDEX_CLASS_NAME,
                        "org.neo4j.index.lucene.LuceneIndexService" );
            }
        }
        
        try
        {
            Object indexService =
                    instantiateOrGetIndexServiceObject( session, out );
            if ( indexService == null )
            {
                throw new ShellException( "No IndexService given, use the " +
                        KEY_INDEX_CLASS_NAME + " environment variable" );
            }
            
            boolean get = parser.options().containsKey( "g" ) ||
                    parser.options().containsKey( "cd" ) ||
                    parser.options().containsKey( "ls" );
            boolean index = parser.options().containsKey( "i" );
            boolean remove = parser.options().containsKey( "r" );
            int count = boolCount( get, index, remove );
            if ( count != 1 )
            {
                throw new ShellException( "Supply one of: -g, -i, -r" );
            }
        
            if ( get )
            {
                get( indexService, parser, session, out );
            }
            else if ( index )
            {
                index( indexService, parser, session, out );
            }
            else if ( remove )
            {
                remove( indexService, parser, session, out );
            }
            return null;
        }
        catch ( ShellException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new ShellException( e );
        }
        finally
        {
            firstRun = false;
        }
    }

    private void get( Object indexService, AppCommandParser parser,
            Session session, Output out ) throws Exception
    {
        String key = parser.arguments().get( 0 );
        String value = parser.arguments().get( 1 );
        @SuppressWarnings( "unchecked" )
        Iterable<Node> result = ( Iterable<Node> )
                indexService.getClass().getMethod( "getNodes",
                String.class, Object.class ).invoke(
                        indexService, key, ( Object ) value );
        boolean doCd = parser.options().containsKey( "cd" );
        boolean doLs = parser.options().containsKey( "ls" );
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
            commandsToRun.addAll( Arrays.asList(
                    commandToRun.split( Pattern.quote( "&&" ) ) ) );
        }
        
        for ( Node node : result )
        {
            printAndInterpretTemplateLines( commandsToRun, false, !specialCommand, node,
                    getServer(), session, out );
        }
    }
    
    private void index( Object indexService, AppCommandParser parser,
            Session session, Output out ) throws Exception
    {
        Node node = getCurrent( session ).asNode();
        String key = parser.arguments().get( 0 );
        Object value = parser.arguments().size() > 1 ?
                parser.arguments().get( 1 ) : node.getProperty( key, null );
        if ( value == null )
        {
            throw new ShellException( "No value to index" );
        }
        indexService.getClass().getMethod( "index", Node.class,
                String.class, Object.class ).invoke( indexService,
                        node, key, value );
    }
    
    private void remove( Object indexService, AppCommandParser parser,
            Session session, Output out ) throws Exception
    {
        Node node = getCurrent( session ).asNode();
        String key = parser.arguments().get( 0 );
        Object value = parser.arguments().size() > 1 ?
                parser.arguments().get( 1 ) : node.getProperty( key, null );
        if ( value == null )
        {
            throw new ShellException( "No value to remove" );
        }
        indexService.getClass().getMethod( "removeIndex", Node.class,
                String.class, Object.class ).invoke( indexService,
                        node, key, value );
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

    private Object instantiateOrGetIndexServiceObject( Session session,
            Output out ) throws Exception
    {
        try
        {
            Class.forName( "org.neo4j.index.IndexService" );
        }
        catch ( Exception e )
        {
            throw new ShellException(
                    "No indexing capabilities on the classpath" );
        }
        
        String className = ( String ) safeGet( session, KEY_INDEX_CLASS_NAME );
        if ( className == null )
        {
            return null;
        }
        
        // TODO OK to synchronize on this?
        Object indexService = null;
        synchronized ( this.indexServices )
        {
            indexService = this.indexServices.get( className );
            if ( indexService == null )
            {
                indexService = Class.forName( className ).getConstructor(
                        GraphDatabaseService.class ).newInstance(
                                this.getServer().getDb() );
                this.indexServices.put( className, indexService );
            }
        }
        return indexService;
    }
}
