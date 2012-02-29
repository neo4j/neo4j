/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.core.LastCommittedTxIdSetter;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultLogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.RecoveryVerifier;

public class CommonFactories
{
    public static class DefaultIdGeneratorFactory implements IdGeneratorFactory
    {
        private final Map<IdType, IdGenerator> generators = new HashMap<IdType, IdGenerator>();
        
        public IdGenerator open( String fileName, int grabSize, IdType idType,
                long highestIdInUse, boolean startup )
        {
            IdGenerator generator = new IdGeneratorImpl( fileName, grabSize, idType.getMaxValue(), idType.allowAggressiveReuse() );
            generators.put( idType, generator );
            return generator;
        }
        
        public IdGenerator get( IdType idType )
        {
            return generators.get( idType );
        }
        
        public void create( String fileName )
        {
            IdGeneratorImpl.createGenerator( fileName );
        }
    }
    
    public static IdGeneratorFactory defaultIdGeneratorFactory()
    {
        return new DefaultIdGeneratorFactory();
    }
    
    public static FileSystemAbstraction defaultFileSystemAbstraction()
    {
        return new DefaultFileSystemAbstraction();
    }
    
    public static LogBufferFactory defaultLogBufferFactory()
    {
        return new DefaultLogBufferFactory();
    }
    
    public static LastCommittedTxIdSetter defaultLastCommittedTxIdSetter()
    {
        return new DefaultLastCommittedTxIdSetter();
    }
    
    public static TxHook defaultTxHook()
    {
        return new DefaultTxHook();
    }
    
    public static RecoveryVerifier defaultRecoveryVerifier()
    {
        return RecoveryVerifier.ALWAYS_VALID;
    }
}
