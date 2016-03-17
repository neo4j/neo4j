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
package cypher;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.experimental.runners.Enclosed;
import org.junit.runners.model.RunnerBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;

public class DownloadedFeatures extends Enclosed
{
    private static final String GITHUB_URL_TO_FEATURE_FILES =
            "https://api.github.com/repos/openCypher/openCypher/contents/tck/features";

    public DownloadedFeatures( Class<?> klass, RunnerBuilder builder ) throws Throwable
    {
        super( klass, download( builder ) );
    }

    private static RunnerBuilder download( RunnerBuilder builder )
    {
        downloadFeatures();
        return builder;
    }

    private static void downloadFeatures()
    {
        File directory = createTargetDirectory();
        byte[] buffer = new byte[1024];
        listGitHubFeatureFiles().forEach( ( name, url ) -> {
            try
            {
                try ( InputStream input = url.openStream();
                      OutputStream output = new FileOutputStream( new File( directory, name ) ) )
                {
                    for ( int read; (read = input.read( buffer )) != -1; )
                    {
                        output.write( buffer, 0, read );
                    }
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Failed to download cypher feature file: " + name, e );
            }
        } );
    }

    private static Map<String,URL> listGitHubFeatureFiles()
    {
        try
        {
            URLConnection con = new URL( GITHUB_URL_TO_FEATURE_FILES ).openConnection();
            // Recommended by GitHub for compatibility
            con.setRequestProperty( "Accept", "application/vnd.github.v3+json" );

            Map<String,URL> urlsToDownload = new HashMap<>();
            try ( JsonParser parser = new JsonFactory( new ObjectMapper() ).createJsonParser( con.getInputStream() ) )
            {
                parser.readValueAsTree().getElements().forEachRemaining(
                        ( file ) -> {
                            String url = file.get( "download_url" ).asText();
                            try
                            {
                                urlsToDownload.put( file.get( "name" ).asText(), new URL( url ) );
                            }
                            catch ( MalformedURLException e )
                            {
                                throw new RuntimeException( "Invalid cypher feature file uri: " + url, e );
                            }
                        } );
            }
            return urlsToDownload;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to list cypher feature files on GitHub.", e );
        }
    }

    private static File createTargetDirectory()
    {
        File directory = new File( "target/features" );
        if ( !directory.exists() )
        {
            if ( !directory.mkdirs() )
            {
                throw new IllegalStateException(
                        "Failed to create target directory for cypher feature files: " + directory );
            }
        }
        return directory;
    }

}
