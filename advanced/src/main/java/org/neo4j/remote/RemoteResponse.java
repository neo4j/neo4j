/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.remote;

import java.io.Serializable;

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

    /**
     * Factory for creating {@link RemoteResponse} objects.
     * 
     * @author Tobias Ivarsson
     */
    public static final class ResponseBuilder implements ResponseVisitor
    {
        /**
         * Create a response for a node request.
         * 
         * @param id
         *            the id of the node.
         * @return The response for the node request.
         */
        public RemoteResponse<NodeSpecification> buildNodeResponse( long id )
        {
            return new NodeResponse( this, id );
        }

        /**
         * Create a response for a relationship request.
         * 
         * @param id
         *            the id of the relationship.
         * @param typeName
         *            the type name of the relationship.
         * @param startNode
         *            the node id of the relationship start node.
         * @param endNode
         *            the node id of the relationship end node.
         * @return The response for the relationship request.
         */
        public RemoteResponse<RelationshipSpecification> buildRelationshipResponse(
            long id, String typeName, long startNode, long endNode )
        {
            return new RelationshipResponse( this, id, typeName, startNode,
                endNode );
        }

        /**
         * Create a response for a property request.
         * 
         * @param value
         *            the value of the property.
         * @return The response for the property request.
         */
        public RemoteResponse<Object> buildPropertyResponse( Object value )
        {
            return new ObjectResponse( this, value );
        }

        /**
         * Create a response for a boolean request.
         * 
         * @param value
         *            the result.
         * @return The response for the boolean request.
         */
        public RemoteResponse<Boolean> buildBooleanResponse( boolean value )
        {
            return new BooleanResponse( this, value );
        }

        /**
         * Create a response for an integer request.
         * 
         * @param value
         *            the result.
         * @return The response for the integer request.
         */
        public RemoteResponse<Integer> buildIntegerResponse( int value )
        {
            return new IntegerResponse( this, value );
        }

        /**
         * Create a partial response for a string iterator request.
         * 
         * @param moreToken
         *            the token used to get the further parts of the iterator.
         * @param strings
         *            the strings to return in this batch.
         * @return The partial response for the string iterator request.
         */
        public RemoteResponse<IterableSpecification<String>> buildPartialStringResponse(
            int moreToken, String... strings )
        {
            return new IterableResponse<String>( this, true, moreToken, -1,
                strings );
        }

        /**
         * Create a final response for a string iterator request.
         * 
         * @param strings
         *            the strings to return in this batch.
         * @return The partial response for the string iterator request.
         */
        public RemoteResponse<IterableSpecification<String>> buildFinalStringResponse(
            String... strings )
        {
            return new IterableResponse<String>( this, false, 0, -1, strings );
        }

        /**
         * Create a partial response for a node iterator request.
         * 
         * @param moreToken
         *            the token used to get the further parts of the iterator.
         * @param size
         *            the total size of the iterable or a negative number for unknown.
         * @param nodes
         *            the nodes to return in this batch.
         * @return The partial response for the node iterator request.
         */
        public RemoteResponse<IterableSpecification<NodeSpecification>> buildPartialNodeResponse(
            int moreToken, long size, NodeSpecification... nodes )
        {
            return new IterableResponse<NodeSpecification>( this, true,
                moreToken, size, nodes );
        }

        /**
         * Create a final response for a node iterator request.
         * @param size
         *            the total size of the iterable or a negative number for unknown.
         * @param nodes
         *            the nodes to return in this batch.
         * @return The partial response for the node iterator request.
         */
        public RemoteResponse<IterableSpecification<NodeSpecification>> buildFinalNodeResponse(
            long size, NodeSpecification... nodes )
        {
            return new IterableResponse<NodeSpecification>( this, false, 0,
                size, nodes );
        }

        /**
         * Create a partial response for a relationship iterator request.
         * 
         * @param moreToken
         *            the token used to get the further parts of the iterator.
         * @param relationships
         *            the relationships to return in this batch.
         * @return The partial response for the relationship iterator request.
         */
        public RemoteResponse<IterableSpecification<RelationshipSpecification>> buildPartialRelationshipResponse(
            int moreToken, RelationshipSpecification... relationships )
        {
            return new IterableResponse<RelationshipSpecification>( this, true,
                moreToken, -1, relationships );
        }

        /**
         * Create a final response for a relationship iterator request.
         * 
         * @param relationships
         *            the relationships to return in this batch.
         * @return The partial response for the relationship iterator request.
         */
        public RemoteResponse<IterableSpecification<RelationshipSpecification>> buildFinalRelationshipResponse(
            RelationshipSpecification... relationships )
        {
            return new IterableResponse<RelationshipSpecification>( this,
                false, 0, -1, relationships );
        }

        /**
         * Create a response for a void request.
         * 
         * @return The response for the void request.
         */
        public RemoteResponse<Void> buildVoidResponse()
        {
            return new VoidResponse( this );
        }

        /**
         * Create an error response for any request.
         * 
         * @param <T>
         *            the type of the original request.
         * @param ex
         *            the exception that occurred during the processing of the
         *            request.
         * @return The error response for the request.
         */
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
        private final long size;
        private final T[] content;

        private IterableResponse( ResponseBuilder builder, boolean hasMore,
            int moreToken, long size, T[] content )
        {
            super( builder );
            this.hasMore = hasMore;
            this.moreToken = moreToken;
            this.size = size;
            this.content = content;
        }

        @Override
        IterableSpecification<T> value()
        {
            return new IterableSpecification<T>( hasMore, moreToken, size,
                content );
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
