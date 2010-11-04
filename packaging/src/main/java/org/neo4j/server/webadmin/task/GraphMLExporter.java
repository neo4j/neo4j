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

package org.neo4j.server.webadmin.task;

import com.tinkerpop.blueprints.pgm.TransactionalGraph;
import com.tinkerpop.blueprints.pgm.parser.GraphMLWriter;
import org.neo4j.rest.domain.DatabaseBlockedException;
import org.neo4j.server.NeoServer;
import org.neo4j.server.webadmin.console.GremlinFactory;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

/**
 * Performs a full export of the underlying database and puts the resulting
 * GraphML file in a subfolder available to the web.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class GraphMLExporter
{

    public static final String EXPORT_FOLDER_PATH = "export";
    public static final String EXPORT_FILE_PATH = "database.gml";

    File     EXPORT_FOLDER;

    File EXPORT_FILE;

    /**
     * Do a full export in GraphML format of the underlying database.
     * 
     * @throws DatabaseBlockedException
     */
    public void doExport() throws DatabaseBlockedException
    {
        EXPORT_FOLDER = new File( new File(
            NeoServer.INSTANCE.configuration().getString(NeoServer.EXPORT_BASE_PATH )), EXPORT_FOLDER_PATH );
        EXPORT_FILE = new File( EXPORT_FOLDER,
            EXPORT_FILE_PATH );
        doExport( EXPORT_FILE );
    }

    private static void doExport( File target ) throws DatabaseBlockedException
    {

        // Since we already have a dependency on Gremlin, we use the GraphML
        // export functionality from
        // there.

        TransactionalGraph graph = GremlinFactory.getGremlinWrappedGraph();

        try
        {

            new File( target.getParent() ).mkdir();

            if ( target.exists() )
            {
                // Delete old export
                target.delete();
            }

            OutputStream stream = new FileOutputStream( target );

            GraphMLWriter.outputGraph( graph, stream );
        }
        catch ( FileNotFoundException e )
        {
            e.printStackTrace();
        }
        catch ( @SuppressWarnings( "restriction" ) XMLStreamException e )
        {
            e.printStackTrace();
        }
    }

}
