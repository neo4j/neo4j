/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.configuration.docs;

import java.io.File;
import java.util.List;
import java.util.function.Function;

import org.neo4j.helpers.Args;
import org.neo4j.io.fs.FileUtils;

/**
 * Generates Asciidoc for the GraphDatabaseSettings class.
 */
public class GenerateConfigDocumentation
{
    public static void main( String[] argv ) throws Exception
    {
        Args arguments = Args.parse( argv );

        File output = arguments.has( "o" ) ? new File(arguments.get("o")) : null;
        List<String> settingsClasses = arguments.orphans();
        if(settingsClasses.size() == 0)
        {
            System.out.println("Usage: GenerateConfigDocumentation [-o output file] SETTINGS_CLASS..");
            System.exit(0);
        }

        String doc = new SettingsDocumenter()
                .document( settingsClasses.stream().map( classFromString ) );

        if(output != null)
        {
            System.out.println("Saving docs in '" + output.getAbsolutePath() + "'.");
            FileUtils.writeToFile(output, doc, false);
        } else
        {
            System.out.println(doc);
        }
    }

    private static Function<String,Class<?>> classFromString = (Function<String,Class<?>>) ( className ) -> {
        try
        {
            return Class.forName( className );
        }
        catch ( ClassNotFoundException e )
        {
            throw new RuntimeException( e );
        }
    };

}
