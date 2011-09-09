/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.test.TestData.Producer;


/**
 * This class is supporting the generation of ASCIIDOC documentation
 * from Java JUnit tests. Snippets can be supplied programmatically in the Java-section
 * and will replace their @@snippetName placeholders in the documentation description.
 * 
 * @author peterneubauer
 *
 */
public class JavaTestDocsGenerator extends AsciiDocGenerator
{
    public static final Producer<JavaTestDocsGenerator> PRODUCER = new Producer<JavaTestDocsGenerator>()
    {
        @Override
        public JavaTestDocsGenerator create( GraphDefinition graph,
                String title, String documentation )
        {
            return (JavaTestDocsGenerator) new JavaTestDocsGenerator( title ).description( documentation );
        }

        @Override
        public void destroy( JavaTestDocsGenerator product, boolean successful )
        {
            // TODO: invoke some complete method here?
        }
    };
    private static final String SNIPPET_MARKER = "@@";
    private Map<String, String> snippets = new HashMap<String, String>();

    public JavaTestDocsGenerator( String title )
    {
        super( title );
    }

    @Override
    protected void writeEntity( FileWriter fw, String entity )
            throws IOException
    {
        // TODO Auto-generated method stub

    }

    public void document( String directory, String sectionName )
    {
        FileWriter fw = getFW( directory + File.separator + sectionName, title );
        String name = title.replace( " ", "-" ).toLowerCase();
        description = replaceSnippets( description );
        try
        {
            line( fw,
                    "[[" + sectionName + "-" + name.replaceAll( "\\(|\\)", "" )
                            + "]]" );
            String firstChar = title.substring( 0, 1 ).toUpperCase();
            line( fw, firstChar + title.substring( 1 ) );
            for ( int i = 0; i < title.length(); i++ )
            {
                fw.append( "=" );
            }
            fw.append( "\n" );
            line( fw, "" );
            line( fw, description );
            line( fw, "" );
            fw.flush();
            fw.close();
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private String replaceSnippets( String description )
    {
        String result = description;
        if ( description.contains( SNIPPET_MARKER ) )
        {
            Pattern p = Pattern.compile( ".*" + SNIPPET_MARKER
                                         + "([a-zA-Z_0-9]*).*" );
            Matcher m = p.matcher( description );
            m.find();
            String group = m.group( 1 );
            if ( !snippets.containsKey( group ) )
            {
                throw new Error( "No snippet '" + group + "' found." );
            }
            result = description.replace( SNIPPET_MARKER + group,
                    snippets.get( group ) );
            result = replaceSnippets( result );
        }
        return result;
    }

    /**
     * Add snippets that will be replaced into corresponding
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

}
