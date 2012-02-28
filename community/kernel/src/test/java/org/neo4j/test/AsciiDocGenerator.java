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
package org.neo4j.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Generate asciidoc-formatted documentation from HTTP requests and responses.
 * The status and media type of all responses is checked as well as the
 * existence of any expected headers.
 * 
 * The filename of the resulting ASCIIDOC test file is derived from the title.
 * 
 * The title is determined by either a JavaDoc period terminated first title
 * line, the @Title annotation or the method name, where "_" is replaced by " ".
 */
public abstract class AsciiDocGenerator
{
    private static final String DOCUMENTATION_END = "\n...\n";
    private Logger log = Logger.getLogger( AsciiDocGenerator.class.getName() );
    protected final String title;
    protected String section;
    protected String description = null;
    protected GraphDatabaseService graph;
    protected static final String SNIPPET_MARKER = "@@";
    protected Map<String, String> snippets = new HashMap<String, String>();

    public AsciiDocGenerator( final String title, final String section )
    {
        this.section = section;
        this.title = title.replace( "_", " " );
    }

    public AsciiDocGenerator setGraph( GraphDatabaseService graph )
    {
        this.graph = graph;
        return this;
    }
    
    public String getTitle()
    {
        return title;
    }
    
    public AsciiDocGenerator setSection(final String section)
    {
        this.section = section;
        return this;
    }


    /**
     * Add a description to the test (in asciidoc format). Adding multiple
     * descriptions will yield one paragraph per description.
     * 
     * @param description the description
     */
    public AsciiDocGenerator description( final String description )
    {
        if ( description == null )
        {
            throw new IllegalArgumentException(
                    "The description can not be null" );
        }
        String content;
        int pos = description.indexOf( DOCUMENTATION_END );
        if ( pos != -1 )
        {
            content = description.substring( 0, pos );
        }
        else
        {
            content = description;
        }
        if ( this.description == null )
        {
            this.description = content;
        }
        else
        {
            this.description += "\n\n" + content;
        }
        return this;
    }

    protected void line( final Writer fw, final String string )
            throws IOException
    {
        fw.append( string );
        fw.append( "\n" );
    }

    public static Writer getFW( String dir, String title )
    {
        try 
        {
            File dirs = new File( dir );
            if ( !dirs.exists() )
            {
                dirs.mkdirs();
            }
            String name = title.replace( " ", "-" ).toLowerCase();
            File out = new File( dirs, name + ".txt" );
            if ( out.exists() )
            {
                out.delete();
            }
            if ( !out.createNewFile() )
            {
                throw new RuntimeException( "File exists: " + out.getAbsolutePath() );
            }
            return new OutputStreamWriter( new FileOutputStream( out, false ),
                    "UTF-8" );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static PrintWriter getPrintWriter( String dir, String title )
    {
        return new PrintWriter( getFW( dir, title ) );
    }
    
    public static String getPath( Class<?> source )
    {
        return source.getPackage()
                .getName()
                .replace( ".", "/" ) + "/" + source.getSimpleName() + ".java";
    }

    protected String replaceSnippets( String description )
    {
        for (String key : snippets.keySet()) {
            description = replaceSnippet( description, key );
        }
        if(description.contains( SNIPPET_MARKER )) {
            int indexOf = description.indexOf( "@@" );
            String snippet = description.substring( indexOf, description.indexOf( "\n", indexOf ) );
            log.severe( "missing snippet ["+snippet+"] in " + description);
        }
        return description;
    }

    private String replaceSnippet( String description, String key )
    {
        String snippetString = SNIPPET_MARKER+key;
        if ( description.contains( snippetString + "\n") )
        {
            description = description.replace( snippetString + "\n",
                    snippets.get( key ) );
        } else {
            log.severe( "could not find " + snippetString + "\\n in "+ description );
        }
        return description;
    }

    /**
     * Add snippets that will be replaced into corresponding.
     * 
     * A snippet needs to be on its own line, terminated by "\n".
     * 
     * @@snippetname placeholders in the content of the description.
     * 
     * @param key the snippet key, without @@
     * @param content the content to be inserted
     */
    public void addSnippet( String key, String content )
    {
        snippets.put( key, content );
    }

    /**
     * Added one or more source snippets from test sources, available from
     * javadoc using
     * 
     * @@tagName.
     * 
     * @param source the class where the snippet is found
     * @param tagNames the tag names which should be included
     */
    public void addTestSourceSnippets( Class<?> source, String... tagNames )
    {
        for ( String tagName : tagNames )
        {
            addSnippet( tagName, sourceSnippet( tagName, source, "test-sources" ) );
        }
    }

    /**
     * Added one or more source snippets, available from javadoc using
     * 
     * @@tagName.
     * 
     * @param source the class where the snippet is found
     * @param tagNames the tag names which should be included
     */
    public void addSourceSnippets( Class<?> source, String... tagNames )
    {
        for ( String tagName : tagNames )
        {
            addSnippet( tagName, sourceSnippet( tagName, source, "sources" ) );
        }
    }

    private static String sourceSnippet( String tagName, Class<?> source,
            String classifier )
    {
        return "[snippet,java]\n" + "----\n"
               + "component=${project.artifactId}\n" + "source="
               + getPath( source ) + "\n" + "classifier=" + classifier + "\n"
               + "tag=" + tagName + "\n" + "----\n";
    }

    public void addGithubTestSourceLink( String key, Class<?> source,
            String repo,
            String dir )
    {
        githubLink( key, source, repo, dir, "test" );
    }

    public void addGithubSourceLink( String key, Class<?> source, String repo,
            String dir )
    {
        githubLink( key, source, repo, dir, "main" );
    }

    private void githubLink( String key, Class<?> source, String repo,
            String dir, String mainOrTest )
    {
        String path = "https://github.com/" + repo
                         + "/blob/{neo4j-git-tag}/";
        if ( dir != null )
        {
            path += dir + "/";
        }
        path += "src/" + mainOrTest + "/java/" + getPath( source );
        path += "[" + source.getSimpleName() + ".java]\n";
        addSnippet( key, path );
    }
}
