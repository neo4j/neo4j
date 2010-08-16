package org.neo4j.visualization.graphviz;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.walk.Walker;

public class Script
{
    public Script( String... format )
    {
        Class<? extends Script> type = getClass();
        for ( String spec : format )
        {
            String[] parts = spec.split( "=", 2 );
            String name = parts[0];
            String[] args = null;
            Method method;
            Throwable error = null;
            try
            {
                if ( parts.length == 1 )
                {
                    method = type.getMethod( name, String[].class );
                }
                else
                {
                    try
                    {
                        method = type.getMethod( name, String.class );
                        args = new String[] { parts[1] };
                    }
                    catch ( NoSuchMethodException nsm )
                    {
                        error = nsm; // use as a flag to know how to invoke
                        method = type.getMethod( name, String[].class );
                        args = parts[1].split( "," );
                    }
                }
                try
                {
                    if ( error == null )
                    {
                        method.invoke( this, (Object[]) args );
                    }
                    else
                    {
                        error = null; // reset the flag use
                        method.invoke( this, (Object) args );
                    }
                }
                catch ( InvocationTargetException ex )
                {
                    error = ex.getTargetException();
                    if ( error instanceof RuntimeException )
                    {
                        throw (RuntimeException) error;
                    }
                }
                catch ( Exception ex )
                {
                    error = ex;
                }
            }
            catch ( NoSuchMethodException nsm )
            {
                error = nsm;
            }
            if ( error != null )
            {
                throw new IllegalArgumentException( "Unknown parameter \""
                                                    + name + "\"", error );
            }
        }
    }

    private List<StyleParameter> styles = new ArrayList<StyleParameter>();

    /**
     * @param args The command line arguments.
     */
    public static void main( String... args )
    {
        if ( args.length < 1 )
        {
            throw new IllegalArgumentException(
                    "GraphvizWriter expects at least one  argument, the path "
                            + "to the Neo4j storage dir." );
        }
        String[] format = new String[args.length - 1];
        System.arraycopy( args, 1, format, 0, format.length );
        Script script = new Script( format );
        script.storeDir( args[0] );
        script.emit( new File( System.getProperty( "graphviz.out", "graph.dot" ) ) );
    }

    private String storeDir;

    private void storeDir( String storeDir )
    {
        this.storeDir = storeDir;
    }

    public final void emit( File outfile )
    {
        GraphDatabaseService graphdb = createGraphDb();
        GraphvizWriter writer = new GraphvizWriter(
                styles.toArray( new StyleParameter[styles.size()] ) );
        try
        {
            Transaction tx = graphdb.beginTx();
            try
            {
                writer.emit( outfile, createGraphWalker( graphdb ) );
            }
            finally
            {
                tx.finish();
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        finally
        {
            graphdb.shutdown();
        }
    }

    protected EmbeddedGraphDatabase createGraphDb()
    {
        return new EmbeddedGraphDatabase( storeDir );
    }

    protected Walker createGraphWalker( GraphDatabaseService graphdb )
    {
        return Walker.fullGraph( graphdb );
    }

    public void nodeTitle( String pattern )
    {
        final PatternParser parser = new PatternParser( pattern );
        styles.add( new StyleParameter.NodeTitle()
        {
            public String getTitle( Node container )
            {
                return parser.parse( container );
            }
        } );
    }

    public void relationshipTitle( String pattern )
    {
        final PatternParser parser = new PatternParser( pattern );
        styles.add( new StyleParameter.RelationshipTitle()
        {
            public String getTitle( Relationship container )
            {
                return parser.parse( container );
            }
        } );
    }

    private static class PatternParser
    {
        private final String pattern;

        PatternParser( String pattern )
        {
            this.pattern = pattern;

        }

        String parse( PropertyContainer container )
        {
            StringBuilder result = new StringBuilder();
            for ( int pos = 0; pos < pattern.length(); )
            {
                char cur = pattern.charAt( pos++ );
                if ( cur == '@' )
                {
                    String key = untilNonAlfa( pos );
                    result.append( getSpecial( key, container ) );
                    pos += key.length();
                }
                else if ( cur == '$' )
                {
                    String key;
                    if ( pattern.charAt( pos ) == '{' )
                    {
                        key = pattern.substring( ++pos, pattern.indexOf( '}',
                                pos++ ) );
                    }
                    else
                    {
                        key = untilNonAlfa( pos );
                    }
                    pos += pattern.length();
                    result.append( container.getProperty( key ) );
                }
                else if ( cur == '\\' )
                {
                    result.append( pattern.charAt( pos++ ) );
                }
                else
                {
                    result.append( cur );
                }
            }
            return result.toString();
        }

        private String untilNonAlfa( int start )
        {
            int end = start;
            while ( Character.isLetter( pattern.charAt( end ) ) )
            {
                end++;
            }
            return pattern.substring( start, end );
        }

        private String getSpecial( String attribute, PropertyContainer container )
        {
            if ( attribute.equals( "id" ) )
            {
                if ( container instanceof Node )
                {
                    return "" + ( (Node) container ).getId();
                }
                else if ( container instanceof Relationship )
                {
                    return "" + ( (Relationship) container ).getId();
                }
            }
            else if ( attribute.equals( "type" ) )
            {
                if ( container instanceof Relationship )
                {
                    return ( (Relationship) container ).getType().name();
                }
            }
            return "@" + attribute;
        }
    }
}
