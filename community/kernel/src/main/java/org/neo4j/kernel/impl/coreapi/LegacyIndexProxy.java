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
package org.neo4j.kernel.impl.coreapi;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.LegacyIndexHits;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.ReadOnlyDatabaseKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.impl.api.legacyindex.AbstractIndexHits;
import org.neo4j.kernel.impl.core.EntityFactory;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import static java.lang.Math.max;
import static java.lang.String.format;

import static org.neo4j.collection.primitive.Primitive.longSet;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.single;
import static org.neo4j.kernel.impl.locking.ResourceTypes.LEGACY_INDEX;
import static org.neo4j.kernel.impl.locking.ResourceTypes.legacyIndexResourceId;

public class LegacyIndexProxy<T extends PropertyContainer> implements Index<T>
{
    public interface Lookup
    {
        GraphDatabaseService getGraphDatabaseService();

        EntityFactory getEntityFactory();
    }

    public static enum Type
    {
        NODE
        {
            @Override
            Class<Node> getEntityType()
            {
                return Node.class;
            }

            @Override
            Node entity( long id, EntityFactory entityFactory )
            {
                return entityFactory.newNodeProxyById( id );
            }

            @Override
            LegacyIndexHits get( ReadOperations operations, String name, String key, Object value )
                    throws LegacyIndexNotFoundKernelException
            {
                return operations.nodeLegacyIndexGet( name, key, value );
            }

            @Override
            LegacyIndexHits query( ReadOperations operations, String name, String key, Object queryOrQueryObject )
                    throws LegacyIndexNotFoundKernelException
            {
                return operations.nodeLegacyIndexQuery( name, key, queryOrQueryObject );
            }

            @Override
            LegacyIndexHits query( ReadOperations operations, String name, Object queryOrQueryObject )
                    throws LegacyIndexNotFoundKernelException
            {
                return operations.nodeLegacyIndexQuery( name, queryOrQueryObject );
            }

            @Override
            void add( DataWriteOperations operations, String name, long id, String key, Object value )
                    throws EntityNotFoundException, InvalidTransactionTypeKernelException,
                    ReadOnlyDatabaseKernelException, LegacyIndexNotFoundKernelException
            {
                operations.nodeAddToLegacyIndex( name, id, key, value );
            }

            @Override
            void remove( DataWriteOperations operations, String name, long id, String key, Object value )
                    throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException,
                    LegacyIndexNotFoundKernelException
            {
                operations.nodeRemoveFromLegacyIndex( name, id, key, value );
            }

            @Override
            void remove( DataWriteOperations operations, String name, long id, String key )
                    throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException,
                    LegacyIndexNotFoundKernelException
            {
                operations.nodeRemoveFromLegacyIndex( name, id, key );
            }

            @Override
            void remove( DataWriteOperations operations, String name, long id )
                    throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException,
                    LegacyIndexNotFoundKernelException
            {
                operations.nodeRemoveFromLegacyIndex( name, id );
            }

            @Override
            void drop( DataWriteOperations operations, String name )
                    throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException,
                    LegacyIndexNotFoundKernelException
            {
                operations.nodeLegacyIndexDrop( name );
            }

            @Override
            long id( PropertyContainer entity )
            {
                return ((Node)entity).getId();
            }
        },
        RELATIONSHIP
        {
            @Override
            Class<Relationship> getEntityType()
            {
                return Relationship.class;
            }

            @Override
            Relationship entity( long id, EntityFactory entityFactory )
            {
                return entityFactory.newRelationshipProxyById( id );
            }

            @Override
            LegacyIndexHits get( ReadOperations operations, String name, String key, Object value )
                    throws LegacyIndexNotFoundKernelException
            {
                return operations.relationshipLegacyIndexGet( name, key, value, -1, -1 );
            }

            @Override
            LegacyIndexHits query( ReadOperations operations, String name, String key, Object queryOrQueryObject )
                    throws LegacyIndexNotFoundKernelException
            {
                return operations.relationshipLegacyIndexQuery( name, key, queryOrQueryObject, -1, -1 );
            }

            @Override
            LegacyIndexHits query( ReadOperations operations, String name, Object queryOrQueryObject )
                    throws LegacyIndexNotFoundKernelException
            {
                return operations.relationshipLegacyIndexQuery( name, queryOrQueryObject, -1, -1 );
            }

            @Override
            void add( DataWriteOperations operations, String name, long id, String key, Object value )
                    throws EntityNotFoundException, InvalidTransactionTypeKernelException,
                    ReadOnlyDatabaseKernelException, LegacyIndexNotFoundKernelException
            {
                operations.relationshipAddToLegacyIndex( name, id, key, value );
            }

            @Override
            void remove( DataWriteOperations operations, String name, long id, String key, Object value )
                    throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException,
                    LegacyIndexNotFoundKernelException
            {
                operations.relationshipRemoveFromLegacyIndex( name, id, key, value );
            }

            @Override
            void remove( DataWriteOperations operations, String name, long id, String key )
                    throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException,
                    LegacyIndexNotFoundKernelException
            {
                operations.relationshipRemoveFromLegacyIndex( name, id, key );
            }

            @Override
            void remove( DataWriteOperations operations, String name, long id )
                    throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException,
                    LegacyIndexNotFoundKernelException
            {
                operations.relationshipRemoveFromLegacyIndex( name, id );
            }

            @Override
            void drop( DataWriteOperations operations, String name ) throws InvalidTransactionTypeKernelException,
                    ReadOnlyDatabaseKernelException, LegacyIndexNotFoundKernelException
            {
                operations.relationshipLegacyIndexDrop( name );
            }

            @Override
            long id( PropertyContainer entity )
            {
                return ((Relationship)entity).getId();
            }
        }

        ;

        abstract <T extends PropertyContainer> Class<T> getEntityType();

        abstract <T extends PropertyContainer> T entity( long id, EntityFactory entityFactory );

        abstract LegacyIndexHits get( ReadOperations operations, String name, String key, Object value )
                throws LegacyIndexNotFoundKernelException;

        abstract LegacyIndexHits query( ReadOperations operations, String name, String key, Object queryOrQueryObject )
                throws LegacyIndexNotFoundKernelException;

        abstract LegacyIndexHits query( ReadOperations operations, String name, Object queryOrQueryObject )
                throws LegacyIndexNotFoundKernelException;

        abstract void add( DataWriteOperations operations, String name, long id, String key, Object value )
                throws EntityNotFoundException, InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException,
                LegacyIndexNotFoundKernelException;

        abstract void remove( DataWriteOperations operations, String name, long id, String key, Object value )
                throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException,
                LegacyIndexNotFoundKernelException;

        abstract void remove( DataWriteOperations operations, String name, long id, String key )
                throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException,
                LegacyIndexNotFoundKernelException;

        abstract void remove( DataWriteOperations operations, String name, long id )
                throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException,
                LegacyIndexNotFoundKernelException;

        abstract void drop( DataWriteOperations operations, String name )
                throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException,
                LegacyIndexNotFoundKernelException;

        abstract long id( PropertyContainer entity );
    }

    protected final String name;
    protected final Type type;
    private final Lookup lookup;
    protected final ThreadToStatementContextBridge statementContextBridge;

    public LegacyIndexProxy( String name, Type type, Lookup lookup,
            ThreadToStatementContextBridge statementContextBridge )
    {
        this.name = name;
        this.type = type;
        this.lookup = lookup;
        this.statementContextBridge = statementContextBridge;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Class<T> getEntityType()
    {
        return type.getEntityType();
    }

    @Override
    public IndexHits<T> get( String key, Object value )
    {
        try ( Statement statement = statementContextBridge.instance() )
        {
            return wrapIndexHits( internalGet( key, value, statement ) );
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    private LegacyIndexHits internalGet( String key, Object value, Statement statement )
            throws LegacyIndexNotFoundKernelException
    {
        return type.get( statement.readOperations(), name, key, value );
    }

    protected IndexHits<T> wrapIndexHits( final LegacyIndexHits ids )
    {
        return new AbstractIndexHits<T>()
        {
            private final PrimitiveLongSet alreadyReturned = longSet( max( 16, ids.size() ) );

            @Override
            public int size()
            {
                return ids.size();
            }

            @Override
            public float currentScore()
            {
                return ids.currentScore();
            }

            @Override
            protected T fetchNextOrNull()
            {
                statementContextBridge.assertInUninterruptedTransaction();
                while ( ids.hasNext() )
                {
                    long id = ids.next();
                    if ( alreadyReturned.add( id ) )
                    {
                        try
                        {
                            return entityOf( id );
                        }
                        catch ( NotFoundException e )
                        {   // By contract this is OK. So just skip it.
                            // TODO 2.2-future this is probably the place to hook in "abandoned id read-repair as well"
                        }
                    }
                }
                return null;
            }
        };
    }

    private T entityOf( long id )
    {
        return type.entity( id, lookup.getEntityFactory() );
    }

    @Override
    public IndexHits<T> query( String key, Object queryOrQueryObject )
    {
        try ( Statement statement = statementContextBridge.instance() )
        {
            return wrapIndexHits( type.query( statement.readOperations(), name, key, queryOrQueryObject ) );
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    @Override
    public IndexHits<T> query( Object queryOrQueryObject )
    {
        try ( Statement statement = statementContextBridge.instance() )
        {
            return wrapIndexHits( type.query( statement.readOperations(), name, queryOrQueryObject ) );
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    @Override
    public boolean isWriteable()
    {
        return true;
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return lookup.getGraphDatabaseService();
    }

    @Override
    public void add( T entity, String key, Object value )
    {
        try ( Statement statement = statementContextBridge.instance() )
        {
            internalAdd( entity, key, value, statement );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( format( "%s %d not found", type, type.id( entity ) ), e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    @Override
    public void remove( T entity, String key, Object value )
    {
        try ( Statement statement = statementContextBridge.instance() )
        {
            type.remove( statement.dataWriteOperations(), name, type.id( entity ), key, value );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    @Override
    public void remove( T entity, String key )
    {
        try ( Statement statement = statementContextBridge.instance() )
        {
            type.remove( statement.dataWriteOperations(), name, type.id( entity ), key );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    @Override
    public void remove( T entity )
    {
        try ( Statement statement = statementContextBridge.instance() )
        {
            type.remove( statement.dataWriteOperations(), name, type.id( entity ) );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    @Override
    public void delete()
    {
        try ( Statement statement = statementContextBridge.instance() )
        {
            type.drop( statement.dataWriteOperations(), name );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    @Override
    public T putIfAbsent( T entity, String key, Object value )
    {
        try ( Statement statement = statementContextBridge.instance() )
        {
            // Does it already exist?
            long existing = single( internalGet( key, value, statement ), -1L );
            if ( existing != -1 )
            {
                return entityOf( existing );
            }

            // No, OK so Grab lock
            statement.readOperations().acquireExclusive( LEGACY_INDEX, legacyIndexResourceId( name, key ) );
            // and check again -- now holding an exclusive lock
            existing = single( internalGet( key, value, statement ), -1L );
            if ( existing != -1 )
            {
                // Someone else created this entry before us just before we got the lock,
                // release the lock as we won't be needing it
                statement.readOperations().releaseExclusive( LEGACY_INDEX, legacyIndexResourceId( name, key ) );
                return entityOf( existing );
            }

            internalAdd( entity, key, value, statement );
            return null;
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( format( "%s %d not found", type, type.id( entity ) ), e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void internalAdd( T entity, String key, Object value, Statement statement ) throws EntityNotFoundException,
            InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException, LegacyIndexNotFoundKernelException
    {
        type.add( statement.dataWriteOperations(), name, type.id( entity ), key, value );
    }
}
