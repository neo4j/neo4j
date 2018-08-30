/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
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
        this.pageCache = StandalonePageCacheFactory.createPageCache( fs, createInitialisedScheduler() );
    }

    private void setupIndex() throws IOException
    {
        tree = new GBPTree<>( pageCache, indexFile, layout, pageSize, NO_MONITOR, NO_HEADER_READER, NO_HEADER_WRITER,
                RecoveryCleanupWorkCollector.immediate() );
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
        consoleInput.add( "restart", new RestartCommand() );
        consoleInput.add( "state", new PrintStateCommand() );
        consoleInput.add( "cc", new ConsistencyCheckCommand() );

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
            return "Print tree";
        }
    }

    private class PrintStateCommand implements Command
    {
        @Override
        public void run( String[] args, PrintStream out ) throws Exception
        {
            tree.printState();
        }

        @Override
        public String toString()
        {
            return "Print state of tree";
        }
    }

    private class Checkpoint implements Command
    {
        @Override
        public void run( String[] args, PrintStream out ) throws Exception
        {
            tree.checkpoint( IOLimiter.UNLIMITED );
        }
        @Override
        public String toString()
        {
            return "Checkpoint tree";
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

    private class RestartCommand implements Command
    {
        @Override
        public void run( String[] args, PrintStream out ) throws Exception
        {
            System.out.println( "Closing tree..." );
            tree.close();
            System.out.println( "Starting tree..." );
            setupIndex();
            System.out.println( "Tree started!" );
        }
        @Override
        public String toString()
        {
            return "Close and open gbptree. No checkpoint is performed.";
        }
    }

    private class ConsistencyCheckCommand implements Command
    {
        @Override
        public void run( String[] args, PrintStream out ) throws Exception
        {
            System.out.println( "Checking consistency..." );
            tree.consistencyCheck();
            System.out.println( "Consistency check finished!");
        }

        @Override
        public String toString()
        {
            return "Check consistency of GBPTree";
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
