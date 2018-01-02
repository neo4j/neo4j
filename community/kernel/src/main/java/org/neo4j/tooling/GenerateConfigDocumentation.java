/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.tooling;

import java.io.File;

import org.neo4j.kernel.configuration.ConfigAsciiDocGenerator;
import org.neo4j.io.fs.FileUtils;

/**
 * Generates Asciidoc for the GraphDatabaseSettings class.
 */
public class GenerateConfigDocumentation
{
    public static void main( String[] args ) throws Exception
    {
    	File output = null;
    	String bundleName = null;
        if(args.length > 0)
        {
        	bundleName = args[0];
        	
        	if(args.length > 1) 
        	{
        		output = new File(args[1]).getAbsoluteFile();
        	}
        	
        } else 
        {
        	System.out.println("Usage: GenerateConfigDocumentation CONFIG_BUNDLE_CLASS [output file]");
        	System.exit(0);
        }
        
        ConfigAsciiDocGenerator generator = new ConfigAsciiDocGenerator();
        String doc = generator.generateDocsFor(bundleName);
        
        if(output != null)
        {
        	System.out.println("Saving docs for '"+bundleName+"' in '" + output.getAbsolutePath() + "'.");
        	FileUtils.writeToFile(output, doc, false);
        } else 
        {
        	System.out.println(doc);
        }
    }
}
