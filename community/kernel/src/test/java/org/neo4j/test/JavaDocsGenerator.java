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
package org.neo4j.test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

public class JavaDocsGenerator extends AsciiDocGenerator
{
    private final static String DIRECTORY = "target" + File.separator + "docs";

    public JavaDocsGenerator( String title, String section )
    {
        super( title, section );
    }
    
    public void saveToFile( String identifier, String text )
    {
        Writer fw = getFW( DIRECTORY + File.separator + this.section,
                getTitle() + "-" + identifier );
        try
        {
            line( fw, text );
            fw.flush();
            fw.close();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }
}
