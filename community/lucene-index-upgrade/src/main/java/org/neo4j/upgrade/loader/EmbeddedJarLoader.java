/*
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
package org.neo4j.upgrade.loader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Loader that will try to load jar that is embedded into some other jar.
 * <p>
 * In case if we do not want jar file to be visible for classloaders we can embed it into other jar as resource.
 * This loaded will allow us to use those jars later on and will provide facility to load classes from them.
 * <p>
 * Please note that this class should not be used as a generic class loader.
 * Example use case: ability to ship several versions of artifact for migration purposes (lucene indexes migrator as
 * example)
 */
public class EmbeddedJarLoader implements AutoCloseable
{
    private String[] jars;
    private Collection<File> extractedFiles;
    private URLClassLoader jarsClassLoader;

    EmbeddedJarLoader( String... jars )
    {
        this.jars = jars;
    }

    /**
     * Load class from embedded jar
     * @param className fully qualified class name
     * @return Loaded class
     * @throws ClassNotFoundException in case if specified class not found
     * @throws IOException if class cannot be extracted
     */
    public Class loadEmbeddedClass( String className ) throws ClassNotFoundException, IOException
    {
        return getJarsClassLoader().loadClass( className );
    }

    /**
     * Get class loaded of embedded jar files
     * @return jar files class loader
     * @throws IOException if exception occurred during class loader construction
     */
    public ClassLoader getJarsClassLoader() throws IOException
    {
        if ( jarsClassLoader == null )
        {
            jarsClassLoader = buildJarClassLoader();
        }
        return jarsClassLoader;
    }

    /**
     * Release class loader that was used for class loading and attempt to delete all extracted jars.
     * If deletion will not succeed, they will be deleted on JVM exit.
     * @throws IOException
     */
    @Override
    public void close() throws IOException
    {
        if ( jarsClassLoader != null )
        {
            jarsClassLoader.close();
        }

        if ( extractedFiles != null )
        {
            extractedFiles.forEach( File::delete );
        }
    }

    private URLClassLoader buildJarClassLoader() throws IOException
    {
        Collection<File> jarFiles = extractJars();
        URL[] urls = jarFiles.stream().map( EmbeddedJarLoader::getFileURL ).toArray( URL[]::new );
        return new URLClassLoader( urls, null );
    }

    private Collection<File> extractJars() throws IOException
    {
        extractedFiles = new ArrayList<>();
        for ( String jar : jars )
        {
            URL url = getClass().getClassLoader().getResource( jar );
            if ( url == null )
            {
                throw new EmbeddedJarNotFoundException( "Jar file '" + jar + "' not found." );
            }
            URLConnection connection = url.openConnection();
            if ( connection instanceof JarURLConnection )
            {
                JarURLConnection urlConnection = (JarURLConnection) connection;
                JarFile jarFile = urlConnection.getJarFile();
                JarEntry jarEntry = urlConnection.getJarEntry();
                if ( jarEntry == null )
                {
                    throw new EmbeddedJarNotFoundException( "Jar file '" + jar + "' not found." );
                }
                File extractedFile = extract( jarFile, jarEntry );
                extractedFiles.add( extractedFile );
            }
            else
            {
                throw new EmbeddedJarNotFoundException( "Jar file '" + jar + "' not found." );
            }
        }
        return this.extractedFiles;
    }

    private File extract( JarFile jarFile, JarEntry jarEntry ) throws IOException
    {
        File extractedFile = createTempFile( jarEntry );
        try ( InputStream jarInputStream = jarFile.getInputStream( jarEntry ) )
        {
            Files.copy( jarInputStream, extractedFile.toPath(), StandardCopyOption.REPLACE_EXISTING );
        }
        return extractedFile;
    }

    private File createTempFile( JarEntry jarEntry ) throws IOException
    {
        File tempFile = File.createTempFile( jarEntry.getName(), "jar" );
        tempFile.deleteOnExit();
        return tempFile;
    }

    private static URL getFileURL( File file )
    {
        try
        {
            return file.toURI().toURL();
        }
        catch ( MalformedURLException e )
        {
            throw new RuntimeException( "Can't convert file " + file + " URI into URL.", e );
        }
    }

}
