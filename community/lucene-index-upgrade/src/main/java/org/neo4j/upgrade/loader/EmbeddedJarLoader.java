/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.upgrade.loader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.CodeSource;
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
    private Collection<File> extractedFiles = new ArrayList<>();
    private URLClassLoader jarsClassLoader;

    EmbeddedJarLoader( String... jars )
    {
        this.jars = jars;
    }

    /**
     * Load class from embedded jar
     *
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
     *
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
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException
    {
        if ( jarsClassLoader != null )
        {
            jarsClassLoader.close();
        }

        extractedFiles.forEach( File::delete );
        extractedFiles.clear();
    }

    private URLClassLoader buildJarClassLoader() throws IOException
    {
        Collection<File> jarFiles = getJars();
        URL[] urls = jarFiles.stream().map( EmbeddedJarLoader::getFileURL ).toArray( URL[]::new );
        return new URLClassLoader( urls, null );
    }

    private Collection<File> getJars() throws IOException
    {
        Collection<File> jarFiles = new ArrayList<>();
        for ( String jar : jars )
        {
            URL url = getClass().getClassLoader().getResource( jar );
            if ( url == null )
            {
                // we can't find jar file as a resource (for example when running from IDE)
                // will try to find it in relative parent directory
                // build should be executed at least once for this to work
                jarFiles.add( loadJarFromRelativePath( jar ) );
            }
            else
            {
                // jar file can be found as resource, lets extract it and use
                File extractedFile = extractJar( url, jar );
                jarFiles.add( extractedFile );
                extractedFiles.add( extractedFile );
            }
        }
        return jarFiles;
    }

    /**
     * Extract jar that is stored ad resource in a parent jar into temporary file
     * @param resourceUrl resource jar resourceUrl
     * @param jar jar resource path
     * @return jar temporary file
     * @throws IOException on exception during jar extractions
     * @throws EmbeddedJarNotFoundException if jar not found or can't be extracted.
     */
    private File extractJar( URL resourceUrl, String jar ) throws IOException
    {
        URLConnection connection = resourceUrl.openConnection();
        if ( connection instanceof JarURLConnection )
        {
            JarURLConnection urlConnection = (JarURLConnection) connection;
            JarFile jarFile = urlConnection.getJarFile();
            JarEntry jarEntry = urlConnection.getJarEntry();
            if ( jarEntry == null )
            {
                throw new EmbeddedJarNotFoundException( "Jar file '" + jar + "' not found." );
            }
            return extract( jarFile, jarEntry );
        }
        else
        {
            throw new EmbeddedJarNotFoundException( "Jar file '" + jar + "' not found." );
        }
    }

    /**
     * Try to load jar from relative ../lib/ directory for cases when we do not have jars in a class path.
     * @param jar - path to a jar file to load.
     * @return loaded jar file
     * @throws EmbeddedJarNotFoundException if jar not exist or file name can't be represented as URI.
     */
    private File loadJarFromRelativePath( String jar )
    {
        try
        {
            CodeSource codeSource = getClass().getProtectionDomain().getCodeSource();
            URI uri = codeSource.getLocation().toURI();
            File jarFile = new File( new File( uri ).getParent(), jar );
            if ( !jarFile.exists() )
            {
                throw new EmbeddedJarNotFoundException( "Jar file '" + jar + "' not found." );
            }
            return jarFile;
        }
        catch ( URISyntaxException e )
        {
            throw new EmbeddedJarNotFoundException( "Jar file '" + jar + "' not found." );
        }
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
