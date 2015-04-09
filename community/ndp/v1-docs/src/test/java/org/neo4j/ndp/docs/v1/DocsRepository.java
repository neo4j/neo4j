/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.docs.v1;

import org.asciidoctor.Asciidoctor;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.function.Function;

import static org.asciidoctor.OptionsBuilder.options;

/**
 * A utility for accessing asciidoc documentation for this module in a semantically meaningful way.
 */
public class DocsRepository
{
    private final static String SEP = File.separator;
    private final static File docsDir = findBackwards( "community" + SEP + "ndp" + SEP + "v1-docs" + SEP +
                                                       "src" + SEP + "docs" + SEP, 5 );

    private final Asciidoctor asciidoc;
    private static final Map<File,Document> docCache = new HashMap<>();

    public static DocsRepository docs()
    {
        return new DocsRepository();
    }

    public DocsRepository()
    {
        asciidoc = Asciidoctor.Factory.create();
    }

    /**
     * Read exerpts from the documentation and parse them into a specified representation.
     *
     * @param fileName is a file name relative to the 'docs' dir of the v1-docs module,
     * for instance 'dev/index.asciidoc'
     * @param cssSelector is a regular css selector, for instance "code[data-lang=\"ndp-struct\"]"
     * @param parser is something that converts the documentation excerpt to your desired representation
     */
    public <T> List<T> read( String fileName, String cssSelector, Function<Element,T> parser )
    {
        List<T> out = new LinkedList<>();
        for ( Element el : doc( fileName ).select( cssSelector ) )
        {
            out.add( parser.apply( el ) );
        }
        return out;
    }

    private Document doc( String fileName )
    {
        File file = new File( docsDir, fileName ).getAbsoluteFile();
        if ( !file.exists() )
        {
            throw new RuntimeException( "Cannot find: " + file.getAbsolutePath() );
        }
        if ( !docCache.containsKey( file ) )
        {
            docCache.put( file, Jsoup.parse( asciidoc.renderFile( file, options().toFile( false ) ) ) );
        }
        return docCache.get( file );
    }

    private static File findBackwards( String dir, int maxDepth )
    {
        if ( maxDepth == 0 )
        {
            throw new RuntimeException( "Couldn't find " + dir );
        }
        else if ( !new File( dir ).exists() )
        {
            return findBackwards( ".." + SEP + dir, maxDepth - 1 );
        }
        else
        {
            return new File( dir );
        }
    }
}
