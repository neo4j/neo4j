/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.util;

import java.util.HashSet;
import java.util.Set;

public class IntArray
{
    private int[] rels;
    private int arrayCount = 0;

    public IntArray() 
    {
        rels = new int[2];
    }
    
    public IntArray( int initialCapacity )
    {
        rels = new int[ initialCapacity ];
    }
    
    public IntArray( int[] array )
    {
        rels = array;
        arrayCount = array.length;
    }
    
    public void add( int id )
    {
        if ( arrayCount == rels.length )
        {
            int newRels[] = new int[rels.length * 2];
            System.arraycopy( rels, 0, newRels, 0, rels.length );
            rels = newRels;
        }
        rels[arrayCount++] = id;
    }
    
    public void addAll( IntArray array )
    {
        if ( array == null )
        {
            return;
        }
        if ( array.length() + arrayCount > rels.length )
        {
            int newSize = rels.length * 2;
            while ( array.length() + arrayCount > newSize )
            {
                newSize = newSize * 2;
            }
            int newRels[] = new int[newSize];
            System.arraycopy( rels, 0, newRels, 0, arrayCount );
            rels = newRels;
        }
        System.arraycopy( array.getArray(), 0, rels, arrayCount, 
            array.length() );
        arrayCount += array.length();
    }
    
    public int length()
    {
        return arrayCount;
    }
    
    public int[] getArray()
    {
        return rels;
    }
    
    public int get( int i )
    {
        assert i >= 0 && i < arrayCount;
        return rels[i];
    }
    
    public static IntArray composeNew( IntArray src, IntArray add, 
        IntArray remove )
    {
        if ( remove == null )
        {
            if ( src == null )
            {
                return add;
            }
            if ( add != null )
            {
                IntArray newArray = new IntArray( add.length() + src.length() );
                newArray.addAll( src );
                newArray.addAll( add );
                return newArray;
            }
            return src;
        }
        else
        {
            if ( src == null && add == null )
            {
                return null;
            }
            int newLength = 0;
            if ( add != null )
            {
                newLength += add.length();
            }
            if ( src != null )
            {
                newLength += src.length();
            }
            IntArray newArray = new IntArray( newLength );
            Set<Integer> set = new HashSet<Integer>( remove.length() + 1, 
                1.0f );
            for ( int i = 0; i < remove.length(); i++ )
            {
                set.add( remove.get( i ) );
            }
            newArray.addAll( src );
            for ( int i = 0; i < newArray.length(); i++ )
            {
                int value = newArray.get( i );
                if ( set.contains( value ) )
                {
                    boolean swapSuccessful = false;
                    for ( int j = newArray.length() - 1; j >= i + 1; j--)
                    {
                        int backValue = newArray.get( j );
                        newArray.arrayCount--;
                        if ( !set.contains( backValue) )
                        {
                            newArray.getArray()[i] = backValue;
                            swapSuccessful = true;
                            break;
                        }
                    }
                    if ( !swapSuccessful ) // all elements from pos in remove
                    {
                        newArray.arrayCount--;
                    }
                }
            }
            if ( add != null )
            {
                for ( int i = 0; i < add.length(); i++ )
                {
                    int value = add.get( i );
                    if ( !set.contains( value ) )
                    {
                        newArray.add( value );
                    }
                }
            }
           return newArray;
        }
    }

/*    public static IntArray composeNew( IntArray src, IntArray add, 
        IntArray remove )
    {
        if ( remove == null )
        {
            if ( src == null )
            {
                return add;
            }
            if ( add != null )
            {
                IntArray newArray = new IntArray( add.length() + src.length() );
                newArray.addAll( src );
                newArray.addAll( add );
                return newArray;
            }
            return src;
        }
        else
        {
            if ( src == null )
            {
                return null;
            }
            // TODO: merge add and remove array then append add array to result
            int[] newArray = new int[src.length()];
            int[] srcArray = src.getArray();
            int[] removeArray = remove.getArray();
            assert removeArray.length <= srcArray.length;
            System.arraycopy( srcArray, 0, newArray, 0, src.length() );
            Arrays.sort( newArray );
            Arrays.sort( removeArray );
            int newArraySize = newArray.length;
            int startSearchFrom = 0;
            int checkTo = removeArray.length; // can decrease if we swap
            for ( int i = 0; i < checkTo; i++ )
            {
                int index = binarySearch( newArray, startSearchFrom, 
                    newArraySize, removeArray[i] );
                if ( index >= 0 )
                {
                    // next search can start from here
                    startSearchFrom = index + 1;
                    
                    // find element we can swap with
                    for ( int j = newArraySize - 1; j >= startSearchFrom; j-- )
                    {
                        int swapValue = newArray[j];
                        int rIndex = binarySearch( removeArray, i, checkTo, 
                            swapValue );

                        newArraySize--;
                        
                        // ok last element in newArray == 
                        // last element in removeArray
                        if ( rIndex > 0 ) 
                        {
                            checkTo--;
                            // continue with second last to see if that is 
                            // swapable
                        }
                        else // we swap with this element
                        {
                            newArray[index] = newArray[j];
                            break;
                        }
                    }
                }
            }
            IntArray newIntArray = new IntArray( newArray );
            newIntArray.arrayCount = newArraySize;
            return newIntArray;
        }
    }
    
    private static int binarySearch( int[] array, int startOffset, 
        int endOffset, int value )
    {
        int pIndex = startOffset + ( endOffset - startOffset ) / 2;

        int valueFound = array[pIndex];
        if ( valueFound == value )
        {
            return pIndex;
        }
        if ( pIndex == startOffset ) // search exhausted
        {
            return -1;
        }
        
        if ( value < valueFound )
        {
            return binarySearch( array, startOffset, pIndex, value );
        }
        else
        {
            return binarySearch( array, pIndex, endOffset, value );
        }
    }*/
}