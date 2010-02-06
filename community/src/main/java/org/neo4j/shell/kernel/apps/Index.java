package org.neo4j.shell.kernel.apps;

import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.SameJvmSession;

public class Index extends GraphDatabaseApp
{
    public static final String KEY_INDEX_CLASS_NAME = "INDEX_CLASS_NAME";
    
    // TODO Can't we fetch these from somewhere?
    private static final Map<String, String> DATA_SOURCE_NAMES = new HashMap<String, String>();
    static
    {
        DATA_SOURCE_NAMES.put( "org.neo4j.index.lucene.LuceneIndexService", "lucene" );
        DATA_SOURCE_NAMES.put( "org.neo4j.index.lucene.LuceneFulltextIndexService",
                "lucene-fulltext" );
    }
    
    private Map<String, IndexServiceContext> contexts =
            new HashMap<String, IndexServiceContext>();
    private boolean firstRun = true;
    
    {
        addValueType( "g", new OptionContext( OptionValueType.NONE,
                "Get nodes for the given key and value" ) );
        addValueType( "i", new OptionContext( OptionValueType.NONE,
                "Index the current node with a key and (optionally) value.\n" +
                "If no value is given the property value for the key is " +
                "used" ) );
        addValueType( "r", new OptionContext( OptionValueType.NONE,
            "Removes a key-value pair for the current node from the index.\n" +
            "If no value is given the property value for the key is used" ) );
    }
    
    public void shutdown()
    {
        for ( IndexServiceContext context : this.contexts.values() )
        {
            if ( !context.instantiatedHere )
            {
                continue;
            }
            
            try
            {
                context.indexService.getClass().getMethod( "shutdown" ).invoke(
                        context.indexService );
            }
            catch ( Exception e )
            {
                // TODO OK?
                System.out.println( "Couldn't shut down index service " +
                        context + ", " + context.getClass() );
            }
        }
        this.contexts.clear();
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
            
            boolean get = parser.options().containsKey( "g" );
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
        String commandToRun = parser.options().get( "c" );
        String[] commandsToRun = commandToRun != null ?
            commandToRun.split( Pattern.quote( "&&" ) ) : new String[ 0 ];
        for ( Node node : result )
        {
            Trav.printAndInterpretTemplateLines( commandsToRun, node,
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
        if ( firstRun )
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
        }
        
        String className = ( String ) safeGet( session, KEY_INDEX_CLASS_NAME );
        if ( className == null )
        {
            return null;
        }
        
        // TODO OK to synchronize on this?
        IndexServiceContext context = null;
        synchronized ( this.contexts )
        {
            context = this.contexts.get( className );
            if ( context == null )
            {
                // Instantiate/get the new IndexService
                if ( session instanceof SameJvmSession )
                {
                    // Instantiate a new one since this is in the same JVM
                    context = new IndexServiceContext( Class.forName( className ).getConstructor(
                            GraphDatabaseService.class ).newInstance(
                                    this.getServer().getDb() ), true );
                }
                else
                {
                    // Look up the IndexService via the XA data source manager
                    XaDataSourceManager xaManager = ( (EmbeddedGraphDatabase) getServer().getDb() )
                            .getConfig().getTxModule().getXaDataSourceManager();
                    String xaName = DATA_SOURCE_NAMES.get( className );
                    if ( xaName == null )
                    {
                        throw new ShellException( "Unrecognized index service " + className );
                    }
                    if ( !xaManager.hasDataSource( xaName ) )
                    {
                        throw new ShellException( "Data source " + className + " not registered" );
                    }
                    
                    // TODO We only support LuceneIndexService (or derivatives), not good
                    XaDataSource dataSource = xaManager.getXaDataSource( xaName );
                    Method getterMethod = dataSource.getClass().getDeclaredMethod(
                            "getIndexService" );
                    getterMethod.setAccessible( true );
                    context = new IndexServiceContext(
                            getterMethod.invoke( dataSource ), false );
                }
                // TODO Check so that it's an IndexService
                this.contexts.put( className, context );
            }
        }
        return context.indexService;
    }
    
    class IndexServiceContext
    {
        private final Object indexService;
        private final boolean instantiatedHere;
        
        IndexServiceContext( Object indexService, boolean instantiatedHere )
        {
            this.indexService = indexService;
            this.instantiatedHere = instantiatedHere;
        }
    }
}
