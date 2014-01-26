/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.runtime;

import java.util.HashMap;
import java.util.Set;

public class MapRegisters implements Registers
{
    public static final RegisterFactory FACTORY = new Factory();

    private final HashMap<Integer, Long> longs;
    private final HashMap<Integer, Object> objects;

    private RegisterSignature signature;

    public MapRegisters()
    {
        this( new RegisterSignature( 0, 0 ) );
    }

    public MapRegisters( RegisterSignature signature )
    {
        this(
            signature,
            new HashMap<Integer, Object>( signature.objectRegisters() ),
            new HashMap<Integer, Long>( signature.entityRegisters() )
        );
    }

    public MapRegisters( RegisterSignature signature,
                         HashMap<Integer, Object> objects, HashMap<Integer, Long> longs )
    {
        this.longs = longs;
        this.objects = objects;

        int maxObjectRegister = maxRegister( signature.objectRegisters(), objects.keySet() );
        int maxLongRegister = maxRegister( signature.entityRegisters(), longs.keySet() );

        this.signature = new RegisterSignature( maxObjectRegister, maxLongRegister );
    }

    @Override
    public Object getObjectRegister( int idx ) {
        return objects.get(idx);
    }

    @Override
    public void setObjectRegister( int idx, Object value ) {
        if ( idx > signature.objectRegisters() )
        {
            signature = signature.withObjectRegisters( idx );
        }
        objects.put(idx, value);
    }

    @Override
    public long getEntityRegister( int idx ) {
        return longs.get(idx);
    }

    @Override
    public void setEntityRegister( int idx, long value ) {
        if ( idx > signature.entityRegisters() )
        {
            signature = signature.withEntityRegisters( idx );
        }
        longs.put(idx, value);
    }

    @Override
    public RegisterFactory factory()
    {
        return FACTORY;
    }

    @Override
    public RegisterSignature signature()
    {
        return signature;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Registers copy() {
        return new MapRegisters(
                signature,
                (HashMap<Integer, Object>)objects.clone(),
                (HashMap<Integer, Long>)longs.clone()
        );
    }

    private static int maxRegister( int initialMaxRegister, Set<Integer> registerKeys )
    {
        int maxRegister = initialMaxRegister;
        for ( Integer key : registerKeys )
        {
            if ( key > maxRegister )
            {
                maxRegister = key;
            }
        }
        return maxRegister;
    }

    private static class Factory implements RegisterFactory
    {
        @Override
        public Registers createRegisters( RegisterSignature signature )
        {
            return new MapRegisters( signature );
        }
    }
}
