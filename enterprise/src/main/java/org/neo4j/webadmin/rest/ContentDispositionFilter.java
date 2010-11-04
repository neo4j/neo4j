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

package org.neo4j.webadmin.rest;

import com.sun.grizzly.arp.AsyncExecutor;
import com.sun.grizzly.arp.AsyncFilter;
import com.sun.grizzly.http.ProcessorTask;
import com.sun.grizzly.tcp.Response;

/**
 * Hack to make grizzly add proper content-disposition headers to GraphML files.
 * 
 * Babies died while writing this. It is done this way because it is meant to be
 * a temporary solution, replaced by something more elegant when the export
 * functionality is moved into neo4j-rest (which uses Jetty6).
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class ContentDispositionFilter implements AsyncFilter
{
    private static final String CONTENT_DISPOSITION = "Content-disposition";
    private static final String ATTACHMENT_DISPOSITION = "attachment;";
    private static final String GRAPHML_SUFFIX = ".gml";

    public boolean doFilter( AsyncExecutor asyncExecutor )
    {
        ProcessorTask task = (ProcessorTask) asyncExecutor.getProcessorTask();

        Response response = task.getRequest().getResponse();

        if ( task.getRequest().requestURI().toString().endsWith( GRAPHML_SUFFIX ) )
        {
            response.addHeader( CONTENT_DISPOSITION, ATTACHMENT_DISPOSITION );
        }

        // Pass execution onwards
        task.invokeAdapter();

        return true;
    }
}
