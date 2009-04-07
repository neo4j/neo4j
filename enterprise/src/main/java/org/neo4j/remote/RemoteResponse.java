/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.remote;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

/**
 * Represents a response to request sent to a {@link RemoteConnection}. The
 * {@link RemoteResponse} contains at least the data that was requested but may
 * also contain additional data. For example data that the client have requested
 * before that have been updated on the server, or data that is very likely that
 * the client will need soon given the request it has made.
 * @author Tobias Ivarsson
 * @param <T>
 *            The type of the requested data
 */
public abstract class RemoteResponse<T> implements Serializable
{
    private static final long serialVersionUID = 1L;

    public static final class ResponseBuilder implements ResponseVisitor
    {
        public RemoteResponse<NodeSpecification> buildNodeResponse( long id )
        {
            return new NodeResponse( this, id );
        }

        public RemoteResponse<RelationshipSpecification> buildRelationshipResponse(
            long id, String typeName, long startNode, long endNode )
        {
            return new RelationshipResponse( this, id, typeName, startNode,
                endNode );
        }

        public RemoteResponse<Object> buildPropertyResponse( Object value )
        {
            return new ObjectResponse( this, value );
        }

        public RemoteResponse<Boolean> buildBooleanResponse( boolean value )
        {
            return new BooleanResponse( this, value );
        }

        public RemoteResponse<Integer> buildIntegerResponse( int value )
        {
            return new IntegerResponse( this, value );
        }

        public RemoteResponse<IterableSpecification<String>> buildPartialStringResponse(
            int moreToken, String... strings )
        {
            return new IterableResponse<String>( this, true, moreToken, strings );
        }

        public RemoteResponse<IterableSpecification<String>> buildFinalStringResponse(
            String... strings )
        {
            return new IterableResponse<String>( this, false, 0, strings );
        }

        public RemoteResponse<IterableSpecification<NodeSpecification>> buildPartialNodeResponse(
            int moreToken, NodeSpecification... nodes )
        {
            return new IterableResponse<NodeSpecification>( this, true,
                moreToken, nodes );
        }

        public RemoteResponse<IterableSpecification<NodeSpecification>> buildFinalNodeResponse(
            NodeSpecification... nodes )
        {
            return new IterableResponse<NodeSpecification>( this, false, 0,
                nodes );
        }

        public RemoteResponse<IterableSpecification<RelationshipSpecification>> buildPartialRelationshipResponse(
            int moreToken, RelationshipSpecification... relationships )
        {
            return new IterableResponse<RelationshipSpecification>( this, true,
                moreToken, relationships );
        }

        public RemoteResponse<IterableSpecification<RelationshipSpecification>> buildFinalRelationshipResponse(
            RelationshipSpecification... relationships )
        {
            return new IterableResponse<RelationshipSpecification>( this,
                false, 0, relationships );
        }

        public RemoteResponse<Void> buildVoidResponse()
        {
            return new VoidResponse( this );
        }

        public <T> RemoteResponse<T> buildErrorResponse( Exception ex )
        {
            return new ErrorResponse<T>( this, ex );
        }
    }

    abstract T value();

    private RemoteResponse( ResponseBuilder builder )
    {
    }

    private static class NodeResponse extends RemoteResponse<NodeSpecification>
    {
        private static final long serialVersionUID = 1L;
        private final long id;

        private NodeResponse( ResponseBuilder builder, long id )
        {
            super( builder );
            this.id = id;
        }

        @Override
        NodeSpecification value()
        {
            return new NodeSpecification( id );
        }
    }

    private static class RelationshipResponse extends
        RemoteResponse<RelationshipSpecification>
    {
        private static final long serialVersionUID = 1L;
        private final long id;
        private final String typeName;
        private final long startNode;
        private final long endNode;

        private RelationshipResponse( ResponseBuilder builder, long id,
            String typeName, long startNode, long endNode )
        {
            super( builder );
            this.id = id;
            this.typeName = typeName;
            this.startNode = startNode;
            this.endNode = endNode;
        }

        @Override
        RelationshipSpecification value()
        {
            return new RelationshipSpecification( id, typeName, startNode,
                endNode );
        }
    }

    private static class IterableResponse<T> extends
        RemoteResponse<IterableSpecification<T>>
    {
        private static final long serialVersionUID = 1L;
        private final boolean hasMore;
        private final int moreToken;
        private final T[] content;

        private IterableResponse( ResponseBuilder builder, boolean hasMore,
            int moreToken, T[] content )
        {
            super( builder );
            this.hasMore = hasMore;
            this.moreToken = moreToken;
            this.content = content;
        }

        @Override
        IterableSpecification<T> value()
        {
            return new IterableSpecification<T>( hasMore, moreToken, content );
        }
    }

    private static class ObjectResponse extends RemoteResponse<Object>
    {
        private static final long serialVersionUID = 1L;
        private final Object value;

        private ObjectResponse( ResponseBuilder builder, Object value )
        {
            super( builder );
            this.value = value;
        }

        @Override
        Object value()
        {
            return value;
        }
    }

    private static class BooleanResponse extends RemoteResponse<Boolean>
    {
        private static final long serialVersionUID = 1L;
        private final boolean value;

        private BooleanResponse( ResponseBuilder builder, boolean value )
        {
            super( builder );
            this.value = value;
        }

        @Override
        Boolean value()
        {
            return value;
        }
    }

    private static class IntegerResponse extends RemoteResponse<Integer>
    {
        private static final long serialVersionUID = 1L;
        private final int value;

        private IntegerResponse( ResponseBuilder builder, int value )
        {
            super( builder );
            this.value = value;
        }

        @Override
        Integer value()
        {
            return value;
        }
    }

    private static class VoidResponse extends RemoteResponse<Void>
    {
        private static final long serialVersionUID = 1L;

        private VoidResponse( ResponseBuilder builder )
        {
            super( builder );
        }

        @Override
        Void value()
        {
            return null;
        }
    }

    private static class ErrorResponse<T> extends RemoteResponse<T>
    {
        private static final long serialVersionUID = 1L;
        private final Exception exception;

        private ErrorResponse( ResponseBuilder builder, Exception ex )
        {
            super( builder );
            this.exception = ex;
        }

        @Override
        T value()
        {
            if ( exception instanceof RuntimeException )
            {
                throw ( RuntimeException ) exception;
            }
            else
            {
                throw new RuntimeException( "Unexpected server exception.",
                    exception );
            }
        }
    }
}
