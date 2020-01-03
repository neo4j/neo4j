/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.tuple.Pair;

import java.io.PrintStream;

import static java.lang.String.format;

public class PrintingGBPTreeVisitor<KEY,VALUE> extends GBPTreeVisitor.Adaptor<KEY,VALUE>
{
    private final PrintStream out;
    private final boolean printValues;
    private final boolean printPosition;
    private final boolean printState;
    private final boolean printHeader;
    private final boolean printFreelist;

    /**
     * Prints a {@link GBPTree} in human readable form, very useful for debugging.
     * Will print sub-tree from that point. Leaves cursor at same page as when called. No guarantees on offset.
     *
     * @param out target to print tree at.
     * @param printPosition whether or not to include positional (slot number) information.
     * @param printState whether or not to also print state pages
     * @param printHeader whether or not to also print header (type, generation, keyCount) of every node
     * @param printFreelist whether or not to also print freelist
     */
    public PrintingGBPTreeVisitor( PrintStream out, boolean printValues, boolean printPosition, boolean printState, boolean printHeader, boolean printFreelist )
    {

        this.out = out;
        this.printValues = printValues;
        this.printPosition = printPosition;
        this.printState = printState;
        this.printHeader = printHeader;
        this.printFreelist = printFreelist;
    }

    @Override
    public void treeState( Pair<TreeState,TreeState> statePair )
    {
        if ( printState )
        {
            out.println( "StateA: " + statePair.getLeft() );
            out.println( "StateB: " + statePair.getRight() );
        }
    }

    @Override
    public void beginLevel( int level )
    {
        out.println( "Level " + level );
    }

    @Override
    public void beginNode( long pageId, boolean isLeaf, long generation, int keyCount )
    {
        if ( printHeader )
        {
            String treeNodeType = isLeaf ? "leaf" : "internal";
            out.print( format( "{%d,%s,generation=%d,keyCount=%d} ",
                    pageId, treeNodeType, generation, keyCount ) );
        }
        else
        {
            out.print( "{" + pageId + "} " );
        }
    }

    @Override
    public void position( int i )
    {
        if ( printPosition )
        {
            out.print( "#" + i + " " );
        }
    }

    @Override
    public void key( KEY key, boolean isLeaf )
    {
        out.print( isLeaf ? key : "[" + key + "]" );
    }

    @Override
    public void value( VALUE value )
    {
        if ( printValues )
        {
            out.print( "=" + value );
        }
        out.print( " " );
    }

    @Override
    public void child( long child )
    {
        out.print( " /" + child + "\\ " );
    }

    @Override
    public void endNode( long pageId )
    {
        out.println();
    }

    @Override
    public void beginFreelistPage( long pageId )
    {
        if ( printFreelist )
        {
            out.print( "Freelist{" + pageId + "} " );
        }
    }

    @Override
    public void endFreelistPage( long pageId )
    {
        if ( printFreelist )
        {
            out.println();
        }
    }

    @Override
    public void freelistEntry( long pageId, long generation, int pos )
    {
        if ( printFreelist )
        {
            out.print( "[" + generation + "," + pageId + "] " );
        }
    }
}
