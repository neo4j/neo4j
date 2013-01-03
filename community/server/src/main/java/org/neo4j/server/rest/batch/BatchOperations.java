/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.rest.batch;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.UriInfo;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.server.rest.web.InternalJettyServletRequest;
import org.neo4j.server.rest.web.InternalJettyServletResponse;
import org.neo4j.server.web.WebServer;

public abstract class BatchOperations
{
    protected static final String ID_KEY = "id";
    protected static final String METHOD_KEY = "method";
    protected static final String BODY_KEY = "body";
    protected static final String TO_KEY = "to";
    protected static final JsonFactory jsonFactory = new JsonFactory();
    protected final WebServer webServer;
    protected final ObjectMapper mapper;

    public BatchOperations( WebServer webServer )
    {
        this.webServer = webServer;
        mapper = new ObjectMapper();
    }

    protected void addHeaders( final InternalJettyServletRequest res,
                               final HttpHeaders httpHeaders )
    {
        for (Map.Entry<String, List<String>> header : httpHeaders
                .getRequestHeaders().entrySet())
        {
            final String key = header.getKey();
            final List<String> value = header.getValue();
            if (value == null)
                continue;
            if (value.size() != 1)
                throw new IllegalArgumentException(
                        "expecting one value per header");
            if ( !key.equals( "Accept" ) && !key.equals( "Content-Type" ) )
            {
                res.addHeader(key, value.get(0));
            }
        }
        // Make sure they are there and always json
        // Taking advantage of Map semantics here
        res.addHeader("Accept", "application/json");
        res.addHeader("Content-Type", "application/json");
    }

    protected URI calculateTargetUri( UriInfo serverUriInfo, String requestedPath )
    {
        URI baseUri = serverUriInfo.getBaseUri();

        if (requestedPath.startsWith(baseUri.toString()))
        {
            requestedPath = requestedPath
                    .substring( baseUri.toString().length() );
        }

        if (!requestedPath.startsWith("/"))
        {
            requestedPath = "/" + requestedPath;
        }

        return baseUri.resolve("." + requestedPath);
    }

    protected String replaceLocationPlaceholders( String str,
                                                  Map<Integer, String> locations )
    {
        if (!str.contains( "{" )) return str;
        // TODO: Potential memory-hog on big requests, write smarter
        // implementation.
        for (Integer jobId : locations.keySet())
        {
            str = str.replace("{" + jobId + "}", locations.get(jobId));
        }
        return str;
    }

    protected boolean is2XXStatusCode( int statusCode )
    {
        return statusCode >= 200 && statusCode < 300;
    }

    protected void parseAndPerform( UriInfo uriInfo, HttpHeaders httpHeaders, InputStream body, Map<Integer, String> locations ) throws IOException, ServletException
    {
        JsonParser jp = jsonFactory.createJsonParser(body);
        JsonToken token;
        while ((token = jp.nextToken()) != null)
        {
            if (token == JsonToken.START_OBJECT)
            {
                String jobMethod="", jobPath="", jobBody="";
                Integer jobId = null;
                while ((token = jp.nextToken()) != JsonToken.END_OBJECT && token != null )
                {
                    String field = jp.getText();
                    jp.nextToken();
                    if (field.equals(METHOD_KEY))
                    {
                        jobMethod = jp.getText().toUpperCase();
                    } else if (field.equals(TO_KEY))
                    {
                        jobPath = jp.getText();
                    } else if (field.equals(ID_KEY))
                    {
                        jobId = jp.getIntValue();
                    } else if (field.equals(BODY_KEY))
                    {
                        jobBody = readBody( jp );

                    }
                }
                // Read one job description. Execute it.
                performRequest( uriInfo, jobMethod, jobPath, jobBody,
                        jobId, httpHeaders, locations );
            }
        }
    }

    private String readBody( JsonParser jp ) throws IOException
    {
        JsonNode node = mapper.readTree( jp );
        StringWriter out = new StringWriter();
        JsonGenerator gen = jsonFactory
                .createJsonGenerator(out);
        mapper.writeTree( gen, node );
        gen.flush();
        gen.close();
        return out.toString();
    }

    protected void performRequest( UriInfo uriInfo, String method, String path, String body, Integer id, HttpHeaders httpHeaders, Map<Integer, String> locations ) throws IOException, ServletException
    {
        path = replaceLocationPlaceholders(path, locations);
        body = replaceLocationPlaceholders(body, locations);
        URI targetUri = calculateTargetUri(uriInfo, path);

        InternalJettyServletRequest req = new InternalJettyServletRequest( method, targetUri.toString(), body);
        addHeaders( req, httpHeaders );

        InternalJettyServletResponse res = new InternalJettyServletResponse();

        invoke( method, path, body, id, targetUri, req, res );
    }

    protected abstract void invoke( String method, String path, String body, Integer id, URI targetUri, InternalJettyServletRequest req, InternalJettyServletResponse res ) throws IOException, ServletException;
}
