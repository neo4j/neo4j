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
package org.neo4j.kernel;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.core.LastCommittedTxIdSetter;
import org.neo4j.kernel.impl.core.RelationshipTypeCreator;
import org.neo4j.kernel.impl.nioneo.store.FileLock;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.transaction.TxModule;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultLogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGeneratorFactory;

public class CommonFactories
{
    public static LockManagerFactory defaultLockManagerFactory()
    {
        return new LockManagerFactory()
        {
            public LockManager create( TxModule txModule )
            {
                return new LockManager( txModule.getTxManager() );
            }
        };
    }
    
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
        
        public void updateIdGenerators( NeoStore neoStore )
        {
            neoStore.updateIdGenerators();
        }
    }
    
    public static IdGeneratorFactory defaultIdGeneratorFactory()
    {
        return new DefaultIdGeneratorFactory();
    }
    
    public static RelationshipTypeCreator defaultRelationshipTypeCreator()
    {
        return new DefaultRelationshipTypeCreator();
    }
    
    public static TxIdGeneratorFactory defaultTxIdGeneratorFactory()
    {
        return new TxIdGeneratorFactory()
        {
            public TxIdGenerator create( final TransactionManager txManager )
            {
                return TxIdGenerator.DEFAULT;
            }
        }; 
    }
    
    public static TxHook defaultTxHook()
    {
        return new TxHook()
        {
            @Override
            public void initializeTransaction( int eventIdentifier )
            {
                // Do nothing from the ordinary here
            }
            
            public boolean hasAnyLocks( Transaction tx )
            {
                return false;
            }
            
            public void finishTransaction( int eventIdentifier, boolean success )
            {
                // Do nothing from the ordinary here
            }
            
            @Override
            public boolean freeIdsDuringRollback()
            {
                return true;
            }
        };
    }
    
    public static LastCommittedTxIdSetter defaultLastCommittedTxIdSetter()
    {
        return new LastCommittedTxIdSetter()
        {
            public void setLastCommittedTxId( long txId )
            {
                // Do nothing
            }
            
            @Override
            public void close()
            {
            }
        };
    }
    
    public static FileSystemAbstraction defaultFileSystemAbstraction()
    {
        return new FileSystemAbstraction()
        {
            @Override
            public FileChannel open( String fileName, String mode ) throws IOException
            {
                return new RandomAccessFile( fileName, mode ).getChannel();
            }
            
            @Override
            public FileLock tryLock( String fileName, FileChannel channel ) throws IOException
            {
                return FileLock.getOsSpecificFileLock( fileName, channel );
            }
        };
    }
    
    public static LogBufferFactory defaultLogBufferFactory()
    {
        return new DefaultLogBufferFactory();
    }
}
