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
package org.neo4j.cypher.internal.javacompat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.CypherException;
import org.neo4j.cypher.CypherExecutionException;
import org.neo4j.cypher.exceptionHandler;
import org.neo4j.cypher.exceptionHandler$;
import org.neo4j.cypher.internal.result.string.ResultStringBuilder;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.values.AnyValue;
import org.neo4j.values.utils.ValuesException;

import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_ONLY;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.WRITE;

public class ResultSubscriber extends PrefetchingResourceIterator<Map<String,Object>> implements QuerySubscriber, Result, QueryExecutionProvider
{
    private final DefaultValueMapper valueMapper;
    private final TransactionalContext context;
    private QueryExecution execution;
    private AnyValue[] currentRecord;
    private Throwable error;
    private QueryStatistics statistics;
    private ResultVisitor<?> visitor;
    private Exception visitException;
    private List<Map<String,Object>> materializeResult;
    private Iterator<Map<String,Object>> materializedIterator;

    public ResultSubscriber( TransactionalContext context )
    {
        this.context = context;
        this.valueMapper = new DefaultValueMapper(
                context.graph().getDependencyResolver().resolveDependency( EmbeddedProxySPI.class ) );
    }

    public void init( QueryExecution execution )
    {
        this.execution = execution;
        assertNoErrors();
        // By policy we materialize the result directly unless it's a read only query.
        QueryExecutionType.QueryType queryType = execution.executionType().queryType();
        if ( queryType != READ_ONLY )
        {
            materializeResult();
            assertNoErrors();
        }

        // ... and if we do not return any rows, we close all resources.
        if ( queryType == WRITE || execution.fieldNames().length == 0 )
        {
            close();
        }
    }

    // QuerySubscriber part
    @Override
    public void onResult( int numberOfFields )
    {
        this.currentRecord = new AnyValue[numberOfFields];
    }

    @Override
    public void onRecord()
    {
    }

    @Override
    public void onField( int offset, AnyValue value )
    {
        currentRecord[offset] = value;
    }

    @Override
    public void onRecordCompleted()
    {
        //We are coming from a call to accept
        if ( visitor != null )
        {
            try
            {
                if ( !visitor.visit( new ResultRowImpl( createPublicRecord() ) ) )
                {
                    execution.cancel();
                    visitor = null;
                }
            }
            catch ( Exception exception )
            {
                this.visitException = exception;
            }
        }

        //we are materializing the result
        if ( materializeResult != null )
        {
            materializeResult.add( createPublicRecord() );
        }
    }

    @Override
    public void onError( Throwable throwable )
    {
        this.error = throwable;
    }

    @Override
    public void onResultCompleted( QueryStatistics statistics )
    {
        this.statistics = statistics;
    }

    // Result part
    @Override
    public QueryExecutionType getQueryExecutionType()
    {
        try
        {
            return execution.executionType();
        }
        catch ( Throwable throwable )
        {
            close();
            throw converted( throwable );
        }
    }

    @Override
    public List<String> columns()
    {
        return Arrays.asList( execution.fieldNames() );
    }

    @Override
    public <T> ResourceIterator<T> columnAs( String name )
    {
        return new ResourceIterator<>()
        {
            @Override
            public void close()
            {
                ResultSubscriber.this.close();
            }

            @Override
            public boolean hasNext()
            {
                return ResultSubscriber.this.hasNext();
            }

            @SuppressWarnings( "unchecked" )
            @Override
            public T next()
            {
                Map<String,Object> next = ResultSubscriber.this.next();
                return (T) next.get( name );
            }
        };
    }

    @Override
    public void close()
    {
        execution.cancel();
    }

    @Override
    public QueryStatistics getQueryStatistics()
    {
        return statistics == null ? QueryStatistics.EMPTY : statistics;
    }

    @Override
    public ExecutionPlanDescription getExecutionPlanDescription()
    {
        try
        {
            return execution.executionPlanDescription();
        }
        catch ( Exception e )
        {
            throw converted( e );
        }
    }

    @Override
    public String resultAsString()
    {
        StringWriter out = new StringWriter();
        PrintWriter writer = new PrintWriter( out );
        writeAsStringTo( writer );
        writer.flush();
        return out.toString();
    }

    @Override
    public void writeAsStringTo( PrintWriter writer )
    {
        ResultStringBuilder stringBuilder =
                ResultStringBuilder.apply( execution.fieldNames(), context );
        try
        {
            accept( stringBuilder );
            stringBuilder.result( writer, statistics );
            for ( Notification notification : getNotifications() )
            {
                writer.println( notification.getDescription() );
            }
        }
        catch ( Exception e )
        {
            close();
            throw converted( e );
        }
    }

    @Override
    public Iterable<Notification> getNotifications()
    {
        return execution.getNotifications();
    }

    @Override
    public <VisitationException extends Exception> void accept( ResultVisitor<VisitationException> visitor )
            throws VisitationException
    {
        if ( materializeResult != null )
        {
            acceptFromMaterialized( visitor );
        }
        else
        {
            acceptFromSubscriber( visitor );
        }
    }

    @Override
    protected Map<String,Object> fetchNextOrNull()
    {
        if ( materializeResult != null )
        {
            return nextFromMaterialized();
        }
        else
        {
            return nextFromSubscriber();
        }
    }

    private Map<String,Object> nextFromMaterialized()
    {
        assertNoErrors();
        if ( materializedIterator == null )
        {
            materializedIterator = materializeResult.iterator();
        }
        if ( materializedIterator.hasNext() )
        {
            return materializedIterator.next();
        }
        else
        {
            return null;
        }
    }

    private Map<String,Object> nextFromSubscriber()
    {
        fetchResults( 1 );
        assertNoErrors();
        if ( hasNewValues() )
        {
            HashMap<String,Object> record = createPublicRecord();
            markAsRead();
            return record;
        }
        else
        {
            return null;
        }
    }

    private boolean hasNewValues()
    {
        return currentRecord.length > 0 && currentRecord[0] != null;
    }

    private void markAsRead()
    {
        if ( currentRecord.length > 0 )
        {
            currentRecord[0] = null;
        }
    }

    private void materializeResult()
    {
        if ( materializeResult == null )
        {
            materializeResult = new ArrayList<>(  );
            fetchResults( Long.MAX_VALUE );
            close();
        }
    }

    private void fetchResults( long numberOfResults )
    {
        try
        {
            execution.request( numberOfResults );
            assertNoErrors();
            execution.await();
        }
        catch ( Exception e )
        {
            close();
            throw converted( e );
        }
    }

    private HashMap<String,Object> createPublicRecord()
    {
        String[] fieldNames = execution.fieldNames();
        HashMap<String,Object> result = new HashMap<>();

        try
        {
            for ( int i = 0; i < fieldNames.length; i++ )
            {
                result.put( fieldNames[i], currentRecord[i].map( valueMapper ) );
            }
        }
        catch ( Throwable t )
        {
            throw converted( t );
        }
        return result;
    }

    private void assertNoErrors()
    {
        if ( error != null )
        {
            close();
            throw converted( error );
        }
    }

    private QueryExecutionException converted( Throwable e )
    {
        CypherException cypherException;
        if ( e instanceof CypherException )
        {
            cypherException = (CypherException) e;
        }
        else if ( e instanceof org.neo4j.cypher.internal.v4_0.util.CypherException )
        {
            cypherException = ((org.neo4j.cypher.internal.v4_0.util.CypherException) e).mapToPublic( exceptionHandler$.MODULE$ );
        }
        else if ( e instanceof ValuesException )
        {
            cypherException = exceptionHandler.mapToCypher( (ValuesException) e );
        }
        else if ( e instanceof ArithmeticException )
        {
            cypherException = exceptionHandler.arithmeticException( e.getMessage(), e );
        }
        else if ( e instanceof RuntimeException )
        {
            throw (RuntimeException) e;
        }
        else
        {
            cypherException = new CypherExecutionException( e.getMessage(), e );
        }
        return new QueryExecutionKernelException( cypherException ).asUserException();
    }

    private <VisitationException extends Exception> void acceptFromMaterialized(
            ResultVisitor<VisitationException> visitor ) throws VisitationException
    {
        assertNoErrors();
        for ( Map<String,Object> materialized : materializeResult )
        {
            if (!visitor.visit( new ResultRowImpl( materialized ) ) )
            {
                break;
            }
        }
    }

    @SuppressWarnings( "unchecked" )
    private <VisitationException extends Exception> void acceptFromSubscriber(
            ResultVisitor<VisitationException> visitor ) throws VisitationException
    {
        this.visitor = visitor;
        fetchResults( Long.MAX_VALUE );
        if ( visitException != null )
        {
            throw (VisitationException) visitException;
        }
        assertNoErrors();
    }

    //TODO please make this go away
    @Override
    public QueryExecution queryExecution()
    {
        return execution;
    }
}
