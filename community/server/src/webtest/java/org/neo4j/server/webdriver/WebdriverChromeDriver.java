/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.webdriver;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.IOUtils;

public class WebdriverChromeDriver
{
    public static final String WEBDRIVER_CHROME_DRIVER = "webdriver.chrome.driver";
    public static final String WEBDRIVER_CHROME_DRIVER_DOWNLOAD_URL = "webdriver.chrome.driver.download.url";

    public static void ensurePresent()
    {
        String chromeDriverPath = System.getProperty( WEBDRIVER_CHROME_DRIVER );
        if (chromeDriverPath == null) {
            throw new IllegalArgumentException( String.format("Please specify system property %s " +
                    "pointing to the location where you expect the chrome driver binary to be present",
                    WEBDRIVER_CHROME_DRIVER) );
        }

        if (new File( chromeDriverPath ).exists()) {
            System.out.println( "Chrome driver found at " + chromeDriverPath );
            return;
        }

        String chromeDriverDownloadUrl = System.getProperty( WEBDRIVER_CHROME_DRIVER_DOWNLOAD_URL );
        if (chromeDriverDownloadUrl ==null) {
            throw new IllegalArgumentException( String.format( "No file present at %s=\"%s\", " +
                    "please specify system property %s for where to fetch it from",
                    WEBDRIVER_CHROME_DRIVER, chromeDriverPath, WEBDRIVER_CHROME_DRIVER_DOWNLOAD_URL ) );
        }

        ZipFile zipFile = downloadFile( chromeDriverDownloadUrl );
        extractTo( zipFile, chromeDriverPath );
    }

    private static ZipFile downloadFile( String fromUrl )
    {
        System.out.println("Downloading binary from " + fromUrl);
        try
        {
            URL zipUrl = new URL( fromUrl );
            ReadableByteChannel rbc = Channels.newChannel( zipUrl.openStream() );
            File localPath = new File( System.getProperty( "java.io.tmpdir" ), "chromedriver.zip" );
            FileOutputStream zipOutputStream = new FileOutputStream( localPath );
            zipOutputStream.getChannel().transferFrom(rbc, 0, 1 << 24);
            return new ZipFile( localPath );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static void extractTo( ZipFile zipFile, String destination )
    {
        System.out.println("Extracting binary to " + destination);
        try
        {
            new File( destination ).getParentFile().mkdirs();
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            InputStream inputStream = zipFile.getInputStream( entries.nextElement() );
            OutputStream outputStream = new BufferedOutputStream( new FileOutputStream( destination ) );
            IOUtils.copy( inputStream, outputStream );
            inputStream.close();
            outputStream.close();

            if (entries.hasMoreElements())
            {
                throw new IllegalStateException( "Unexpected additional entries in zip file" );
            }

            new File( destination ).setExecutable( true );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

}
