/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.coreapi;

import java.util.NoSuchElementException;
import java.util.function.Supplier;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.ExplicitIndexWrite;
import org.neo4j.internal.kernel.api.NodeExplicitIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.locking.ResourceTypes.explicitIndexResourceId;

public class ExplicitIndexProxy<T extends PropertyContainer> implements Index<T>
{
    public static final Type NODE = new Type<Node>()
    {
        @Override
        public Class<Node> getEntityType()
        {
            return Node.class;
        }

        @Override
        public Node entity( long id, GraphDatabaseService graphDatabaseService )
        {
            return graphDatabaseService.getNodeById( id );
        }

        @Override
        public IndexHits<Node> get( KernelTransaction ktx, String name, String key, Object value,
                GraphDatabaseService graphDatabaseService ) throws ExplicitIndexNotFoundKernelException
        {
            NodeExplicitIndexCursor cursor = ktx.cursors().allocateNodeExplicitIndexCursor();
            ktx.indexRead().nodeExplicitIndexLookup( cursor, name, key, value );
            return new CursorWrappingNodeIndexHits( cursor, graphDatabaseService, ktx, name );
        }

        @Override
        public IndexHits<Node> query( KernelTransaction ktx, String name, String key, Object queryOrQueryObject,
                GraphDatabaseService graphDatabaseService )
                throws ExplicitIndexNotFoundKernelException
        {
            NodeExplicitIndexCursor cursor = ktx.cursors().allocateNodeExplicitIndexCursor();
            ktx.indexRead().nodeExplicitIndexQuery( cursor, name, key, queryOrQueryObject );
            return new CursorWrappingNodeIndexHits( cursor, graphDatabaseService, ktx, name );
        }

        @Override
        public IndexHits<Node> query( KernelTransaction ktx, String name, Object queryOrQueryObject,
                GraphDatabaseService graphDatabaseService )
                throws ExplicitIndexNotFoundKernelException
        {
            NodeExplicitIndexCursor cursor = ktx.cursors().allocateNodeExplicitIndexCursor();
            ktx.indexRead().nodeExplicitIndexQuery( cursor, name, queryOrQueryObject );
            return new CursorWrappingNodeIndexHits( cursor, graphDatabaseService, ktx, name );
        }

        @Override
        public void add( ExplicitIndexWrite operations, String name, long id, String key, Object value )
                throws ExplicitIndexNotFoundKernelException
        {
            operations.nodeAddToExplicitIndex( name, id, key, value );
        }

        @Override
        public void remove( ExplicitIndexWrite operations, String name, long id, String key, Object value )
                throws ExplicitIndexNotFoundKernelException
        {
            operations.nodeRemoveFromExplicitIndex( name, id, key, value );
        }

        @Override
        public void remove( ExplicitIndexWrite operations, String name, long id, String key )
                throws ExplicitIndexNotFoundKernelException
        {
            operations.nodeRemoveFromExplicitIndex( name, id, key );
        }

        @Override
        public void remove( ExplicitIndexWrite operations, String name, long id )
                throws ExplicitIndexNotFoundKernelException
        {
            operations.nodeRemoveFromExplicitIndex( name, id );
        }

        @Override
        public void drop( ExplicitIndexWrite operations, String name )
                throws ExplicitIndexNotFoundKernelException
        {
            operations.nodeExplicitIndexDrop( name );
        }

        @Override
        public long id( PropertyContainer entity )
        {
            return ((Node) entity).getId();
        }
    };

    public static final Type RELATIONSHIP = new Type<Relationship>()
    {
        @Override
        public Class<Relationship> getEntityType()
        {
            return Relationship.class;
        }

        @Override
        public Relationship entity( long id, GraphDatabaseService graphDatabaseService )
        {
            return graphDatabaseService.getRelationshipById( id );
        }

        @Override
        public IndexHits<Relationship> get( KernelTransaction ktx, String name, String key, Object value,
            GraphDatabaseService graphDatabaseService ) throws ExplicitIndexNotFoundKernelException
        {
            RelationshipExplicitIndexCursor cursor = ktx.cursors().allocateRelationshipExplicitIndexCursor();
            ktx.indexRead().relationshipExplicitIndexLookup( cursor, name, key, value, -1, -1 );
            return new CursorWrappingRelationshipIndexHits( cursor, graphDatabaseService, ktx, name );
        }

        @Override
        public IndexHits<Relationship> query( KernelTransaction ktx, String name, String key, Object queryOrQueryObject,
            GraphDatabaseService graphDatabaseService )
                throws ExplicitIndexNotFoundKernelException
        {
            RelationshipExplicitIndexCursor cursor = ktx.cursors().allocateRelationshipExplicitIndexCursor();
            ktx.indexRead().relationshipExplicitIndexQuery( cursor, name, key, queryOrQueryObject,-1, -1 );
            return new CursorWrappingRelationshipIndexHits( cursor, graphDatabaseService, ktx, name );
        }

        @Override
        public IndexHits<Relationship> query( KernelTransaction ktx, String name, Object queryOrQueryObject,
            GraphDatabaseService graphDatabaseService )
                throws ExplicitIndexNotFoundKernelException
        {
            RelationshipExplicitIndexCursor cursor = ktx.cursors().allocateRelationshipExplicitIndexCursor();
            ktx.indexRead().relationshipExplicitIndexQuery( cursor, name, queryOrQueryObject, -1 , -1 );
            return new CursorWrappingRelationshipIndexHits( cursor, graphDatabaseService, ktx, name );
        }

        @Override
        public void add( ExplicitIndexWrite operations, String name, long id, String key, Object value )
                throws ExplicitIndexNotFoundKernelException, EntityNotFoundException
        {
            operations.relationshipAddToExplicitIndex( name, id, key, value );
        }

        @Override
        public void remove( ExplicitIndexWrite operations, String name, long id, String key, Object value )
                throws ExplicitIndexNotFoundKernelException
        {
            operations.relationshipRemoveFromExplicitIndex( name, id, key, value );
        }

        @Override
        public void remove( ExplicitIndexWrite operations, String name, long id, String key )
                throws ExplicitIndexNotFoundKernelException
        {
            operations.relationshipRemoveFromExplicitIndex( name, id, key );
        }

        @Override
        public void remove( ExplicitIndexWrite operations, String name, long id )
                throws ExplicitIndexNotFoundKernelException
        {
            operations.relationshipRemoveFromExplicitIndex( name, id );
        }

        @Override
        public void drop( ExplicitIndexWrite operations, String name )
                throws ExplicitIndexNotFoundKernelException
        {
            operations.relationshipExplicitIndexDrop( name );
        }

        @Override
        public long id( PropertyContainer entity )
        {
            return ((Relationship) entity).getId();
        }
    };

    interface Type<T extends PropertyContainer>
    {
        Class<T> getEntityType();

        T entity( long id, GraphDatabaseService graphDatabaseService );

        IndexHits<T> get( KernelTransaction operations, String name, String key, Object value,
                GraphDatabaseService gds ) throws ExplicitIndexNotFoundKernelException;

        IndexHits<T> query( KernelTransaction operations, String name, String key, Object queryOrQueryObject,
                GraphDatabaseService gds ) throws ExplicitIndexNotFoundKernelException;

        IndexHits<T> query( KernelTransaction transaction, String name, Object queryOrQueryObject,
                GraphDatabaseService gds ) throws ExplicitIndexNotFoundKernelException;

        void add( ExplicitIndexWrite operations, String name, long id, String key, Object value )
                throws ExplicitIndexNotFoundKernelException, EntityNotFoundException;

        void remove( ExplicitIndexWrite operations, String name, long id, String key, Object value )
                throws ExplicitIndexNotFoundKernelException;

        void remove( ExplicitIndexWrite operations, String name, long id, String key )
                throws ExplicitIndexNotFoundKernelException;

        void remove( ExplicitIndexWrite operations, String name, long id ) throws ExplicitIndexNotFoundKernelException;

        void drop( ExplicitIndexWrite operations, String name ) throws ExplicitIndexNotFoundKernelException;

        long id( PropertyContainer entity );
    }

    public interface Lookup
    {
        GraphDatabaseService getGraphDatabaseService();
    }

    protected final String name;
    protected final Type<T> type;
    protected final Supplier<KernelTransaction> txBridge;
    private final GraphDatabaseService gds;

    public ExplicitIndexProxy( String name, Type<T> type, GraphDatabaseService gds,
                             Supplier<KernelTransaction> txBridge )
    {
        this.name = name;
        this.type = type;
        this.gds = gds;
        this.txBridge = txBridge;
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
        KernelTransaction ktx = txBridge.get();
        try ( Statement ignore = ktx.acquireStatement() )
        {
            return internalGet( key, value, ktx ) ;
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    private IndexHits<T> internalGet( String key, Object value, KernelTransaction ktx )
            throws ExplicitIndexNotFoundKernelException
    {
        return type.get( ktx, name, key, value, gds );
    }

    @Override
    public IndexHits<T> query( String key, Object queryOrQueryObject )
    {
        KernelTransaction ktx = txBridge.get();
        try ( Statement ignore = ktx.acquireStatement() )
        {
            return type.query( ktx, name, key, queryOrQueryObject, gds );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    @Override
    public IndexHits<T> query( Object queryOrQueryObject )
    {
        KernelTransaction ktx = txBridge.get();
        try ( Statement ignore = ktx.acquireStatement() )
        {
            return type.query( ktx, name, queryOrQueryObject, gds );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
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
        return gds;
    }

    @Override
    public void add( T entity, String key, Object value )
    {
        KernelTransaction ktx = txBridge.get();
        try ( Statement ignore = ktx.acquireStatement() )
        {
            internalAdd( entity, key, value, ktx );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( format( "%s %d not found", type, type.id( entity ) ), e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    @Override
    public void remove( T entity, String key, Object value )
    {
        KernelTransaction ktx = txBridge.get();
        try ( Statement ignore = ktx.acquireStatement() )
        {
            type.remove( ktx.indexWrite(), name, type.id( entity ), key, value );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    @Override
    public void remove( T entity, String key )
    {
        KernelTransaction ktx = txBridge.get();
        try ( Statement ignore = ktx.acquireStatement() )
        {
            type.remove( ktx.indexWrite(), name, type.id( entity ), key );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    @Override
    public void remove( T entity )
    {
        KernelTransaction ktx = txBridge.get();
        try ( Statement ignore = ktx.acquireStatement() )
        {
            internalRemove( ktx, type.id( entity ) );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    private void internalRemove( KernelTransaction ktx, long id )
            throws InvalidTransactionTypeKernelException, ExplicitIndexNotFoundKernelException
    {
        type.remove( ktx.indexWrite(), name, id );
    }

    @Override
    public void delete()
    {
        KernelTransaction ktx = txBridge.get();
        try ( Statement ignore = ktx.acquireStatement() )
        {
            type.drop( ktx.indexWrite(), name );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    @Override
    public T putIfAbsent( T entity, String key, Object value )
    {
        KernelTransaction ktx = txBridge.get();
        try ( Statement ignore = ktx.acquireStatement() )
        {
            // Does it already exist?
            T existing = Iterators.single( internalGet( key, value, ktx ), null );
            if ( existing != null )
            {
                return existing;
            }

            // No, OK so Grab lock
            ktx.locks().acquireExclusiveExplicitIndexLock( explicitIndexResourceId( name, key ) );
            // and check again -- now holding an exclusive lock
            existing = Iterators.single( internalGet( key, value, ktx ), null );
            if ( existing != null )
            {
                // Someone else created this entry before us just before we got the lock,
                // release the lock as we won't be needing it
                ktx.locks().releaseExclusiveExplicitIndexLock( explicitIndexResourceId( name, key ) );
                return existing;
            }

            internalAdd( entity, key, value, ktx );
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
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void internalAdd( T entity, String key, Object value, KernelTransaction transaction ) throws EntityNotFoundException,
            InvalidTransactionTypeKernelException, ExplicitIndexNotFoundKernelException
    {
        type.add( transaction.indexWrite(), name, type.id( entity ), key, value );
    }

    @Override
    public String toString()
    {
        return "Index[" + type + ", " + name + "]";
    }

    private abstract static class AbstractCursorWrappingIndexHits<T extends PropertyContainer> implements IndexHits<T>
    {
        private static final long NOT_INITIALIZED = -2L;
        static final long NO_ID = -1L;
        private long next = NOT_INITIALIZED;
        protected int size;
        protected float score;

        AbstractCursorWrappingIndexHits( int size, float score )
        {
            this.size = size;
            this.score = score;
        }

        @Override
        public ResourceIterator<T> iterator()
        {
            return this;
        }

        @Override
        public int size()
        {
            return size;
        }

        @Override
        public float currentScore()
        {
            return score;
        }

        @Override
        public T getSingle()
        {
            if ( !hasNext() )
            {
                return null;
            }

            T item = next();
            if ( hasNext() )
            {
                throw new NoSuchElementException();
            }
            return item;
        }

        @Override
        public boolean hasNext()
        {
            if ( next == NOT_INITIALIZED )
            {
                next = fetchNext();
            }
            return next != NO_ID;
        }

        @Override
        public T next()
        {
            if ( !hasNext() )
            {
                close();
                throw new NoSuchElementException();
            }
            T item = materialize( next );
            next = NOT_INITIALIZED;
            return item;
        }

        protected abstract long fetchNext();

        protected abstract T materialize( long id );
    }

    private static class CursorWrappingNodeIndexHits extends AbstractCursorWrappingIndexHits<Node>
    {
        private final NodeExplicitIndexCursor cursor;
        private final GraphDatabaseService graphDatabaseService;
        private final KernelTransaction ktx;
        private final String name;

        private CursorWrappingNodeIndexHits( NodeExplicitIndexCursor cursor,
                GraphDatabaseService graphDatabaseService, KernelTransaction ktx, String name )
        {
            super( cursor.expectedTotalNumberOfResults(), cursor.score() );
            this.cursor = cursor;
            this.graphDatabaseService = graphDatabaseService;
            this.ktx = ktx;
            this.name = name;
        }

        @Override
        public void close()
        {
            cursor.close();
        }

        protected long fetchNext()
        {
            ktx.assertOpen();
            while ( cursor.next() )
            {
                long reference = cursor.nodeReference();
                if ( ktx.dataRead().nodeExists( reference ) )
                {
                    return reference;
                }
                else if ( ktx.securityContext().mode().allowsWrites() )
                {
                    //remove it from index so it doesn't happen again
                    try
                    {
                        NODE.remove( ktx.indexWrite(), name, reference );
                    }
                    catch ( ExplicitIndexNotFoundKernelException | InvalidTransactionTypeKernelException e )
                    {
                        //ignore
                    }
                }
            }
            close();
            return NO_ID;
        }

        @Override
        protected Node materialize( long id )
        {
            this.score = cursor.score();
            this.size = cursor.expectedTotalNumberOfResults();
            return graphDatabaseService.getNodeById( id );
        }
    }

    protected static class CursorWrappingRelationshipIndexHits extends AbstractCursorWrappingIndexHits<Relationship>
    {
        private final RelationshipExplicitIndexCursor cursor;
        private final GraphDatabaseService graphDatabaseService;
        private final KernelTransaction ktx;
        private final String name;

        CursorWrappingRelationshipIndexHits( RelationshipExplicitIndexCursor cursor,
                GraphDatabaseService graphDatabaseService, KernelTransaction ktx, String name )
        {
            super( cursor.expectedTotalNumberOfResults(), cursor.score() );
            this.cursor = cursor;
            this.graphDatabaseService = graphDatabaseService;
            this.ktx = ktx;
            this.name = name;
        }

        @Override
        public void close()
        {
            cursor.close();
        }

        protected long fetchNext()
        {
            ktx.assertOpen();
            while ( cursor.next() )
            {
                long reference = cursor.relationshipReference();
                if ( ktx.dataRead().relationshipExists( reference ) )
                {
                    return reference;
                }
                else if ( ktx.securityContext().mode().allowsWrites() )
                {
                    //remove it from index so it doesn't happen again
                    try
                    {
                        RELATIONSHIP.remove( ktx.indexWrite(), name, reference );
                    }
                    catch ( ExplicitIndexNotFoundKernelException | InvalidTransactionTypeKernelException e )
                    {
                        //ignore
                    }
                }
            }
            close();
            return NO_ID;
        }

        @Override
        protected Relationship materialize( long id )
        {
            this.score = cursor.score();
            this.size = cursor.expectedTotalNumberOfResults();
            return graphDatabaseService.getRelationshipById( id );
        }
    }
}
