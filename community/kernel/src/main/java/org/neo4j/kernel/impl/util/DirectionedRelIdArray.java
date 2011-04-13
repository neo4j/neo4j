/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.neo4j.graphdb.Direction;


public class DirectionedRelIdArray
{
    private final RelIdArray out;
    private final RelIdArray in;
    
    public DirectionedRelIdArray()
    {
        this( new RelIdArray(), new RelIdArray() );
    }

    public DirectionedRelIdArray( RelIdArray out, RelIdArray in )
    {
        this.out = out;
        this.in = in;
    }
    
    public void addOut( long id )
    {
        out.add( id );
    }
    
    public void addIn( long id )
    {
        in.add( id );
    }
    
    public void addAllOut( RelIdArray ids )
    {
        out.addAll( ids );
    }
    
    public void addAllIn( RelIdArray ids )
    {
        in.addAll( ids );
    }
    
    public void addAll( DirectionedRelIdArray ids )
    {
        out.addAll( ids.out );
        in.addAll( ids.in );
    }
    
    private RelIdArray combined()
    {
        return new CombinedRelIdArray();
    }
    
    private class CombinedRelIdArray extends RelIdArray
    {
        CombinedRelIdArray()
        {
            addBlocks( out );
            addBlocks( in );
        }
        
        @Override
        public void addAll( RelIdArray array )
        {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public void add( long id )
        {
            throw new UnsupportedOperationException();
        }
    }

    public RelIdArray getOut()
    {
        return out;
    }
    
    public RelIdArray getIn()
    {
        return in;
    }
    
    public static DirectionedRelIdArray from( DirectionedRelIdArray src,
            DirectionedRelIdArray add, RelIdArray remove )
    {
        if ( add == null && remove == null )
        {
            return src;
        }
        return new DirectionedRelIdArray(
                RelIdArray.from( src != null ? src.out : null, add != null ? add.out : null, remove ),
                RelIdArray.from( src != null ? src.in : null, add != null ? add.in : null, remove ) );
    }
    
    public static DirectionWrapper wrap( Direction direction )
    {
        switch ( direction )
        {
        case OUTGOING: return DirectionWrapper.OUT;
        case INCOMING: return DirectionWrapper.IN;
        default: return DirectionWrapper.BOTH;
        }
    }
    
    public static enum DirectionWrapper
    {
        OUT
        {
            @Override
            public void add( DirectionedRelIdArray target, long id )
            {
                target.out.add( id );
            }

            @Override
            public void addAll( DirectionedRelIdArray target, RelIdArray ids )
            {
                target.out.addAll( ids );
            }

            @Override
            public RelIdArray get( DirectionedRelIdArray target )
            {
                return target.out;
            }
        },
        IN
        {
            @Override
            public void add( DirectionedRelIdArray target, long id )
            {
                target.in.add( id );
            }

            @Override
            public void addAll( DirectionedRelIdArray target, RelIdArray ids )
            {
                target.in.addAll( ids );
            }

            @Override
            public RelIdArray get( DirectionedRelIdArray target )
            {
                return target.in;
            }
        },
        BOTH
        {
            @Override
            public void add( DirectionedRelIdArray target, long id )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void addAll( DirectionedRelIdArray target, RelIdArray ids )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public RelIdArray get( DirectionedRelIdArray target )
            {
                return target.combined();
            }
        };
        
        public abstract void add( DirectionedRelIdArray target, long id );
        
        public abstract void addAll( DirectionedRelIdArray target, RelIdArray ids );
        
        public abstract RelIdArray get( DirectionedRelIdArray target );
    }
}
