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
package org.neo4j.kernel.impl.util;

public class RelIdArrayWithLoops extends RelIdArray
{
    private IdBlock loopBlock;

    public RelIdArrayWithLoops( int type )
    {
        super( type );
    }

    @Override
    public int sizeOfObjectInBytesIncludingOverhead()
    {
        return super.sizeOfObjectInBytesIncludingOverhead() + sizeOfBlockWithReference( loopBlock );
    }

    protected RelIdArrayWithLoops( RelIdArray from )
    {
        super( from );
        loopBlock = from.getLastLoopBlock();
    }

    protected RelIdArrayWithLoops( int type, IdBlock out, IdBlock in, IdBlock loop )
    {
        super( type, out, in );
        this.loopBlock = loop;
    }

    @Override
    protected IdBlock getLastLoopBlock()
    {
        return this.loopBlock;
    }

    @Override
    protected void setLastLoopBlock( IdBlock block )
    {
        this.loopBlock = block;
    }

    @Override
    public RelIdArray upgradeIfNeeded( RelIdArray capabilitiesToMatch )
    {
        return this;
    }

    @Override
    public RelIdArray downgradeIfPossible()
    {
        return loopBlock == null ? new RelIdArray( this ) : this;
    }

    @Override
    public RelIdArray newSimilarInstance()
    {
        return new RelIdArrayWithLoops( getType() );
    }

    @Override
    protected boolean accepts( RelIdArray source )
    {
        return true;
    }
}