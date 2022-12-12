/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.security;

import static java.net.HttpURLConnection.HTTP_NOT_MODIFIED;

import inet.ipaddr.IPAddressString;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.graphdb.security.URLAccessValidationError;

public class WebURLAccessRule implements URLAccessRule
{
    public static final String LOAD_CSV_USER_AGENT_PREFIX = "NeoLoadCSV_";
    private static final int REDIRECT_LIMIT = 10;

    public static String userAgent()
    {
        var version = Runtime.version();
        var agent = System.getProperty( "http.agent" );
        if ( agent == null )
        {
            return "Java/" + version;
        }
        return agent + " Java/" + version;
    }

    // This is used by APOC and thus needs to be public
    public static void checkNotBlocked( URL url, List<IPAddressString> blockedIpRanges ) throws Exception
    {
        InetAddress inetAddress = InetAddress.getByName( url.getHost() );

        for ( var blockedIpRange : blockedIpRanges )
        {
            if ( blockedIpRange.contains( new IPAddressString( inetAddress.getHostAddress() ) ) )
            {
                throw new URLAccessValidationError( "access to " + inetAddress + " is blocked via the configuration property "
                                                    + GraphDatabaseInternalSettings.cypher_ip_blocklist.name() );
            }
        }
    }

    public static HttpURLConnection checkUrlIncludingHops( URL url, List<IPAddressString> blockedIpRanges )
            throws Exception
    {
        URL result = url;
        boolean isRedirect;
        int redirectLimit = REDIRECT_LIMIT;
        HttpURLConnection con;

        do
        {
            // We need to validate each intermediate url if there are redirects.
            // Otherwise, we could have situations like an internal ip, e.g. 10.0.0.1
            // is banned in the config, but it redirects to another different internal ip
            // and we would still have a security hole
            checkNotBlocked( result, blockedIpRanges );
            con = (HttpURLConnection) result.openConnection();
            con.setRequestProperty("User-Agent", String.format("%s%s", LOAD_CSV_USER_AGENT_PREFIX, userAgent()));
            con.setInstanceFollowRedirects( false );
            con.connect();
            con.getInputStream();
            isRedirect = isRedirect(con.getResponseCode());

            if ( isRedirect )
            {
                if ( redirectLimit-- == 0 )
                {
                    con.disconnect();
                    throw new IOException( "Redirect limit exceeded" );
                }
                String location = con.getHeaderField( "Location" );

                if ( location == null )
                {
                    con.disconnect();
                    throw new IOException( "URL responded with a redirect but the location header was null" );
                }

                URL newUrl;
                try
                {
                    newUrl = new URL( location );
                    if ( !newUrl.getProtocol().equalsIgnoreCase(result.getProtocol()) )
                    {
                        con.disconnect();
                        return con;
                    }
                }
                catch ( MalformedURLException e )
                {
                    // Try to use the location as a relative path, matches browser behaviour
                    newUrl = new URL( con.getURL(), location );
                }
                result = newUrl;
            }

            con.disconnect();
        }
        while ( isRedirect );

        return con;
    }

    private static boolean isRedirect( int responseCode )
    {
        return responseCode >= 300 && responseCode <= 307 && responseCode != 306 && responseCode != HTTP_NOT_MODIFIED;
    }

    @Override
    public URL validate( Configuration config, URL url ) throws URLAccessValidationError
    {
        List<IPAddressString> blockedIpRanges = config.get( GraphDatabaseInternalSettings.cypher_ip_blocklist );
        String host = url.getHost();
        if ( !blockedIpRanges.isEmpty() && host != null && !host.isEmpty() )
        {
            try
            {
                checkUrlIncludingHops( url, blockedIpRanges );
            }
            catch ( Exception e )
            {
                throw new URLAccessValidationError( "Unable to verify access to " + host + ". Cause: " + e.getMessage() );
            }
        }
        return url;
    }
}
