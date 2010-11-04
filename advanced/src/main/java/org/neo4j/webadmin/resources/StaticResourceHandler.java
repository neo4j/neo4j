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

package org.neo4j.webadmin.resources;

import static org.neo4j.webadmin.utils.FileUtils.getFileAsString;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Takes care of assembling static file sets (js, css and template files) into
 * combined text blocks that can be sent rapidly to the client.
 * 
 * This implementation currently stores its result in a String, so this can
 * under no circumstances be used if you are dealing with filesets with a
 * combined size larger than a few megabytes.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class StaticResourceHandler
{

    protected File rootFolder;
    protected File resourcesFile;
    protected long resouresFileLastModified = 0;
    protected StaticResourceCompiler compiler;

    /**
     * How often, in milliseconds, we should check if any files have changed.
     */
    protected long compileCheckInterval = 1000;

    /**
     * A list of all files that are to be included in the compiled file, in the
     * order we want them to appear in.
     */
    protected volatile ArrayList<File> resources = new ArrayList<File>();

    /**
     * Lookup table to see when files had been changed the previous time we
     * checked. Used to determine if a file has been changed since last compile.
     */
    protected HashMap<File, Long> resourceLastModifiedMap = new HashMap<File, Long>();

    /**
     * In-memory storage of compiled result.
     * 
     * TODO: Switch to File in order to handle bigger applications.
     */
    protected String compiledString = "";

    /**
     * Keep track of whether to run the dirty check task or not.
     */
    private boolean running = true;

    private boolean isCompiling = false;

    /**
     * This is triggered to check if files on disk have changed, and thus our
     * end-file needs to be recompiled.
     */
    private TimerTask compileTriggerTask = new TimerTask()
    {
        public void run()
        {
            if ( !running )
            {
                this.cancel();
            }
            else
            {
                if ( resourcesFile.lastModified() != resouresFileLastModified )
                {
                    reloadResourceList();
                    resouresFileLastModified = resourcesFile.lastModified();
                }

                boolean dirty = false;
                for ( File resource : resources )
                {
                    long lastModified = resource.lastModified();
                    if ( !resourceLastModifiedMap.containsKey( resource )
                         || resourceLastModifiedMap.get( resource ) != lastModified )
                    {
                        resourceLastModifiedMap.put( resource, lastModified );
                        dirty = true;
                    }
                }

                if ( !isCompiling && dirty )
                {
                    compile();
                }
            }
        }
    };

    //
    // CONSTRUCTORS
    //

    /**
     * Create a new instance. This will add rootFolder as a base for all file
     * paths found in the resources file. It will also use rootFolder as a base
     * to find resourcesFile itself.
     */
    public StaticResourceHandler( String rootFolder,
            String resourcesFileRelativePath )
    {
        this( new File( rootFolder ), new File( new File( rootFolder ),
                resourcesFileRelativePath ) );
    }

    public StaticResourceHandler( File rootFolder, File resourcesFile )
    {
        this( rootFolder, resourcesFile, new FileJoinCompiler() );
    }

    public StaticResourceHandler( File rootFolder, File resourcesFile,
            StaticResourceCompiler compiler )
    {
        this.rootFolder = rootFolder;
        this.resourcesFile = resourcesFile;
        this.compiler = compiler;

        reloadResourceList();

        Timer timer = new Timer( "resourcesCompileCheck" );
        timer.scheduleAtFixedRate( compileTriggerTask, 0, compileCheckInterval );
    }

    //
    // PUBLIC
    //

    public String getCompiled()
    {
        if ( isCompiling )
        {
            waitForCompile();
        }

        return compiledString;

    }

    public void start()
    {
        running = true;
        Timer timer = new Timer( "resourcesDirtyCheck" );
        timer.scheduleAtFixedRate( compileTriggerTask, 0, compileCheckInterval );
    }

    public void stop()
    {
        this.running = false;
    }

    public boolean isRunning()
    {
        return running;
    }

    //
    // INTERNALS
    //

    private synchronized void compile()
    {
        try
        {
            isCompiling = true;
            compiledString = null;
            StringBuilder builder = new StringBuilder();
            for ( File file : resources )
            {
                compiler.addFile( builder, file );
            }
            compiledString = builder.toString();

            builder = null;
        }
        finally
        {
            isCompiling = false;
        }
    }

    private void waitForCompile()
    {
        try
        {
            while ( isCompiling )
            {
                Thread.sleep( 6 );
            }
        }
        catch ( InterruptedException e )
        {
            // Nop
        }
    }

    protected void reloadResourceList()
    {
        try
        {
            resources.clear();
            final String[] names = getFileAsString( resourcesFile ).split( "\n" );

            for ( String name : names )
            {
                if ( name.trim().length() > 0 && !name.startsWith( "#" ) )
                {
                    resources.add( new File( rootFolder, name ) );
                }
            }

        }
        catch ( IOException e )
        {
            throw new RuntimeException(
                    "Unable to read resource list file "
                            + resourcesFile.getAbsolutePath() + ".", e );
        }
    }
}
