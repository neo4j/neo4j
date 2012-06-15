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

package org.neo4j.tooling;

import java.io.File;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettingsResourceBundle;
import org.neo4j.kernel.configuration.ConfigAsciiDocGenerator;
import org.neo4j.kernel.impl.util.FileUtils;

/**
 * Generates Asciidoc for the GraphDatabaseSettings class.
 */
public class GenerateNeo4jSettingsAsciidoc
{
    public static void main( String[] args ) throws Exception
    {
        ConfigAsciiDocGenerator generator = new ConfigAsciiDocGenerator();
        String doc = generator.generateDocsFor(GraphDatabaseSettingsResourceBundle.class);
        
        if(args.length > 0)
        {
        	File output = new File(args[0]);
        	System.out.println("Saving docs for '"+GraphDatabaseSettings.class.getName()+"' in '" + output.getAbsolutePath() + "'.");
        	FileUtils.writeToFile(output, doc, false);
        } else 
        {
        	System.out.println(doc);
        }
    }
}
