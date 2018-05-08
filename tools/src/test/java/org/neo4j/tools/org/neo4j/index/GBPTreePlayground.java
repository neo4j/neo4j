/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.tools.org.neo4j.index;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.SimpleLongLayout;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.tools.console.input.Command;
import org.neo4j.tools.console.input.ConsoleInput;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.tools.console.input.ConsoleUtil.staticPrompt;

public class GBPTreePlayground
{
    private final File indexFile;
    private GBPTree<MutableLong,MutableLong> tree;

    private int pageSize = 256;
    private PageCache pageCache;
    private SimpleLongLayout layout;
    private final MutableBoolean autoPrint = new MutableBoolean( true );

    private GBPTreePlayground( FileSystemAbstraction fs, File indexFile )
    {
        this.indexFile = indexFile;
        this.layout = SimpleLongLayout.longLayout().build();
        this.pageCache = StandalonePageCacheFactory.createPageCache( fs );
    }

    private void setupIndex() throws IOException
    {
        tree = new GBPTree<>( pageCache, indexFile, layout, pageSize, NO_MONITOR, NO_HEADER_READER, NO_HEADER_WRITER,
                RecoveryCleanupWorkCollector.IMMEDIATE );
    }

    private void run() throws InterruptedException, IOException
    {
        System.out.println( "Working on: " + indexFile.getAbsolutePath() );
        setupIndex();

        LifeSupport life = new LifeSupport();
        ConsoleInput consoleInput = life.add( new ConsoleInput( System.in, System.out, staticPrompt( "# " ) ) );
        consoleInput.add( "print", new Print() );
        consoleInput.add( "add", new AddCommand() );
        consoleInput.add( "remove", new RemoveCommand() );
        consoleInput.add( "cp", new Checkpoint() );
        consoleInput.add( "autoprint", new ToggleAutoPrintCommand() );

        life.start();
        try
        {
            consoleInput.waitFor();
        }
        finally
        {
            life.shutdown();
        }
    }

    private class Print implements Command
    {
        @Override
        public void run( String[] args, PrintStream out ) throws Exception
        {
            tree.printTree();
        }

        @Override
        public String toString()
        {
            return "print tree";
        }
    }

    private class Checkpoint implements Command
    {
        @Override
        public void run( String[] args, PrintStream out ) throws Exception
        {
            tree.checkpoint( IOLimiter.unlimited() );
        }

        @Override
        public String toString()
        {
            return "checkpoint tree";
        }
    }

    private class AddCommand implements Command
    {
        @Override
        public void run( String[] args, PrintStream out ) throws Exception
        {
            Long[] longValues = new Long[args.length];
            for ( int i = 0; i < args.length; i++ )
            {
                longValues[i] = Long.valueOf( args[i] );
            }
            MutableLong key = new MutableLong();
            MutableLong value = new MutableLong();
            try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
            {
                for ( Long longValue : longValues )
                {
                    key.setValue( longValue );
                    value.setValue( longValue );
                    writer.put( key, value );
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            maybePrint();
        }

        @Override
        public String toString()
        {
            return "N [N ...] (add key N)";
        }
    }

    private class RemoveCommand implements Command
    {
        @Override
        public void run( String[] args, PrintStream out ) throws Exception
        {
            Long[] longValues = new Long[args.length];
            for ( int i = 0; i < args.length; i++ )
            {
                longValues[i] = Long.valueOf( args[i] );
            }
            MutableLong key = new MutableLong();
            try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
            {
                for ( Long longValue : longValues )
                {
                    key.setValue( longValue );
                    writer.remove( key );
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            maybePrint();
        }

        @Override
        public String toString()
        {
            return "N [N ...] (remove key N)";
        }
    }

    private class ToggleAutoPrintCommand implements Command
    {
        @Override
        public void run( String[] args, PrintStream out )
        {
            if ( autoPrint.isTrue() )
            {
                autoPrint.setFalse();
            }
            else
            {
                autoPrint.setTrue();
            }
        }

        @Override
        public String toString()
        {
            return "Toggle auto print after modifications (ON by default)";
        }
    }

    private void maybePrint() throws IOException
    {
        if ( autoPrint.getValue() )
        {
            print();
        }
    }

    private void print() throws IOException
    {
        tree.printTree();
    }

    public static void main( String[] args ) throws InterruptedException, IOException
    {
        File indexFile;
        if ( args.length > 0 )
        {
            indexFile = new File( args[0] );
        }
        else
        {
            indexFile = new File( "index" );
        }

        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        new GBPTreePlayground( fs, indexFile ).run();
    }
}
