/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.webadmin.task;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import javax.xml.stream.XMLStreamException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.rest.domain.DatabaseBlockedException;
import org.neo4j.rest.domain.DatabaseLocator;
import org.neo4j.webadmin.parser.GraphMLReader;

@SuppressWarnings( "restriction" )
public class GraphMLImporter
{

    public static void doImport( String filename ) throws IOException,
            DatabaseBlockedException, XMLStreamException
    {
        InputStream stream;
        try
        {
            stream = new URL( filename ).openStream();
        }
        catch ( MalformedURLException urlEx )
        {
            stream = new FileInputStream( filename );
        }

        doImport( stream );
        stream.close();
    }

    /**
     * Do a full export in GraphML format of the underlying database.
     * 
     * @throws DatabaseBlockedException
     * @throws XMLStreamException
     */
    public static void doImport( InputStream stream )
            throws DatabaseBlockedException, XMLStreamException
    {

        // Since we already have a dependency on Gremlin, we use the GraphML
        // import functionality from
        // there.

        GraphDatabaseService graph = DatabaseLocator.getGraphDatabase();

        GraphMLReader.inputGraph( graph, stream );
    }

}
