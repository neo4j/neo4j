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
package org.neo4j.shell.kernel.apps;

import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.ColumnPrinter;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.helpers.collection.Iterables.concat;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.indexOf;
import static org.neo4j.helpers.collection.Iterables.sort;
import static org.neo4j.shell.Continuation.INPUT_COMPLETE;

public class Schema extends TransactionProvidingApp
{
    private static final String INDENT = "  ";

    private static final Function<IndexDefinition,String> LABEL_COMPARE_FUNCTION =
            index -> index.getLabel().name();

    {
        addOptionDefinition( "l", new OptionDefinition( OptionValueType.MUST,
                "Specifies which label selected operation is about" ) );
        addOptionDefinition( "r", new OptionDefinition( OptionValueType.MUST,
                "Specifies which relationship type selected operation is about" ) );
        addOptionDefinition( "p", new OptionDefinition( OptionValueType.MUST,
                "Specifies which property selected operation is about" ) );
        addOptionDefinition( "a", new OptionDefinition( OptionValueType.NONE,
                "Used together with schema sample to indicate that all indexes should be sampled" ) );
        addOptionDefinition( "f", new OptionDefinition( OptionValueType.NONE,
                "Used together with schema sample to force indexes to be sampled" ) );
        addOptionDefinition( "v", new OptionDefinition( OptionValueType.NONE,
                "Verbose output of failure descriptions etc." ) );
    }

    @Override
    public String getDescription()
    {
        return "Accesses db schema. Usage: schema <action> <options...>\n" +
               "Listing indexes\n" +
               "  schema ls\n" +
               "  schema ls -l :Person\n" +
               "  schema ls -r :KNOWS\n" +
               "Sample all indexes\n" +
               "  schema sample -a\n" +
               "Sample a specific index\n" +
               "  schema sample -l :Person -p name\n" +
               "  schema sample -r :KNOWS -p since\n" +
               "Force a sampling of a specific index\n" +
               "  schema sample -f -l :Person -p name\n" +
               "Awaiting indexes to come online\n" +
               "  schema await -l :Person -p name\n" +
               "Print indexing progress\n" +
               "  schema progress\n" +
               "  schema progress -l :Person\n" +
               "  schema progress -l :Person -p name";
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out ) throws Exception
    {
        String action = parser.argumentWithDefault( 0, "ls" );
        org.neo4j.graphdb.schema.Schema schema = getServer().getDb().schema();
        Label[] labels = parseLabels( parser );
        RelationshipType[] relTypes = parseRelTypes( parser );
        String property = parser.option( "p", null );
        boolean sampleAll = parser.options().containsKey( "a" );
        boolean forceSample = parser.options().containsKey( "f" );
        boolean verbose = parser.options().containsKey( "v" );

        switch ( action )
        {
        case "await":
            if ( relTypes.length > 0 )
            {
                throw new ShellException( "It is only possible to await nodes related index" );
            }
            awaitIndexes( out, schema, labels, property );
            break;
        case "progress":
            if ( relTypes.length > 0 )
            {
                throw new ShellException( "It is only possible to show progress on nodes related index" );
            }
            printIndexProgress( out, schema, labels, property );
            break;
        case "ls":
            listIndexesAndConstraints( out, schema, labels, relTypes, property, verbose );
            break;
        case "sample":
            if ( relTypes.length > 0 )
            {
                throw new ShellException( "It is only possible to sample nodes related index" );
            }
            sampleIndexes( labels, property, sampleAll, forceSample );
            break;
        default:
            out.println( "Unknown action: " + action + "\nUSAGE:\n" + getDescription() );
            break;
        }

        return INPUT_COMPLETE;
    }

    private void listIndexesAndConstraints( Output out, org.neo4j.graphdb.schema.Schema schema, Label[] labels,
            RelationshipType[] relTypes, String property, boolean verbose ) throws RemoteException
    {
        if ( labels.length > 0 && relTypes.length == 0 )
        {
            listNodeIndexesAndConstraints( out, schema, labels, property, verbose );
        }
        else if ( relTypes.length > 0 && labels.length == 0 )
        {
            listRelationshipIndexesAndConstraints( out, schema, relTypes, property, verbose );
        }
        else
        {
            listAllIndexesAndConstraints( out, schema, labels, relTypes, property, verbose );
        }
    }

    private void sampleIndexes( Label[] labels, String property, boolean sampleAll, boolean forceSample )
            throws ShellException
    {

        IndexingService indexingService = getServer().getDb().getDependencyResolver().resolveDependency(
                IndexingService.class );
        if ( indexingService == null )
        {
            throw new ShellException( "Internal error: failed to resolve IndexingService" );
        }

        IndexSamplingMode samplingMode = getSamplingMode( forceSample );

        // Trigger sampling for all indices
        if ( sampleAll )
        {
            indexingService.triggerIndexSampling( samplingMode );
            return;
        }

        validateLabelsAndProperty( labels, property );

        int labelKey;
        int propertyKey;

        TokenRead tokenRead = getServer().getTokenRead();
        labelKey = tokenRead.nodeLabel( labels[0].name() );
        propertyKey = tokenRead.propertyKey( property );

        if ( labelKey == -1 )
        {
            throw new ShellException( "No label associated with '" + labels[0].name() + "' was found" );
        }
        if ( propertyKey == -1 )
        {
            throw new ShellException( "No property associated with '" + property + "' was found" );
        }

        try
        {
            indexingService.triggerIndexSampling( SchemaDescriptorFactory.forLabel( labelKey, propertyKey ),
                    samplingMode );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new ShellException( e.getMessage() );
        }
    }

    private IndexSamplingMode getSamplingMode( boolean forceSample )
    {
        if ( forceSample )
        {
            return IndexSamplingMode.TRIGGER_REBUILD_ALL;
        }
        else
        {
            return IndexSamplingMode.TRIGGER_REBUILD_UPDATED;
        }
    }

    private void validateLabelsAndProperty( Label[] labels, String property ) throws ShellException
    {
        if ( labels.length == 0 && property == null )
        {
            throw new ShellException( "Invalid usage of sample. \nUSAGE:\n" + getDescription() );
        }

        if ( labels.length > 1 )
        {
            throw new ShellException( "Only one label must be provided" );
        }

        // If we provide one we must also provide the other
        if ( property == null || labels.length == 0 )
        {
            throw new ShellException( "Provide both the property and the label, or run with -a to sample all indexes" );
        }
    }

    private void printIndexProgress( Output out, org.neo4j.graphdb.schema.Schema schema, Label[] labels,
            String property ) throws RemoteException
    {
        for ( IndexDefinition index : indexesByLabelAndProperty( schema, labels, property ) )
        {
            out.println( String.format( "%s: %1.1f%%", index.getLabel().name(),
                    schema.getIndexPopulationProgress( index ).getCompletedPercentage() ) );
        }
    }

    private void awaitIndexes( Output out, org.neo4j.graphdb.schema.Schema schema, Label[] labels, String property )
            throws RemoteException
    {
        for ( IndexDefinition index : indexesByLabelAndProperty( schema, labels, property ) )
        {
            if ( schema.getIndexState( index ) != IndexState.ONLINE )
            {
                out.println( String.format( "Awaiting :%s ON %s %s", index.getLabel().name(),
                        asList( index.getPropertyKeys() ), IndexState.ONLINE ) );
                schema.awaitIndexOnline( index, 10000, TimeUnit.DAYS );
            }
        }
    }

    private void listNodeIndexesAndConstraints( Output out, org.neo4j.graphdb.schema.Schema schema, Label[] labels,
            String property, boolean verbose ) throws RemoteException
    {
        reportNodeIndexes( out, schema, labels, property, verbose );
        reportNodeConstraints( out, schema, labels, property );
    }

    private void listRelationshipIndexesAndConstraints( Output out, org.neo4j.graphdb.schema.Schema schema,
            RelationshipType[] types, String property, boolean verbose ) throws RemoteException
    {
        // no relationship indexes atm
        reportRelationshipConstraints( out, schema, types, property );
    }

    private void listAllIndexesAndConstraints( Output out, org.neo4j.graphdb.schema.Schema schema, Label[] labels,
            RelationshipType[] types, String property, boolean verbose ) throws RemoteException
    {
        reportNodeIndexes( out, schema, labels, property, verbose );
        reportAllConstraints( out, schema, labels, types, property );
    }

    private void reportNodeConstraints( Output out, org.neo4j.graphdb.schema.Schema schema,
            Label[] labels, String property ) throws RemoteException
    {
        Iterable<ConstraintDefinition> nodeConstraints = constraintsByLabelAndProperty( schema, labels, property );
        reportConstraints( out, nodeConstraints );
    }

    private void reportRelationshipConstraints( Output out, org.neo4j.graphdb.schema.Schema schema,
            RelationshipType[] types, String property ) throws RemoteException
    {
        Iterable<ConstraintDefinition> relConstraints = constraintsByTypeAndProperty( schema, types, property );
        reportConstraints( out, relConstraints );
    }

    private void reportAllConstraints( Output out, org.neo4j.graphdb.schema.Schema schema,
            Label[] labels, RelationshipType[] types, String property ) throws RemoteException
    {
        Iterable<ConstraintDefinition> allConstraints = concat(
                constraintsByLabelAndProperty( schema, labels, property ),
                constraintsByTypeAndProperty( schema, types, property ) );

        reportConstraints( out, allConstraints );
    }

    private void reportConstraints( Output out, Iterable<ConstraintDefinition> constraints ) throws RemoteException
    {
        int j = 0;
        for ( ConstraintDefinition constraint : constraints )
        {
            if ( j == 0 )
            {
                out.println();
                out.println( "Constraints" );
            }

            out.println( indent( constraint.toString() ) );
            j++;

        }
        if ( j == 0 )
        {
            out.println();
            out.println( "No constraints" );
        }
    }

    private void reportNodeIndexes( Output out, org.neo4j.graphdb.schema.Schema schema, Label[] labels, String property,
            boolean verbose ) throws RemoteException
    {
        ColumnPrinter printer = new ColumnPrinter( indent( "ON " ), "", "" );
        Iterable<IndexDefinition> indexes = indexesByLabelAndProperty( schema, labels, property );

        int i = 0;
        for ( IndexDefinition index : sort( indexes, LABEL_COMPARE_FUNCTION ) )
        {
            if ( i == 0 )
            {
                out.println( "Indexes" );
            }
            String labelAndProperties = String.format( ":%s(%s)", index.getLabel().name(), commaSeparate( index
                    .getPropertyKeys() ) );

            IndexState state = schema.getIndexState( index );
            String uniqueOrNot = index.isConstraintIndex() ? "(for uniqueness constraint)" : "";

            printer.add( labelAndProperties, state, uniqueOrNot );
            if ( verbose && state == IndexState.FAILED )
            {
                printer.addRaw( schema.getIndexFailure( index ) );
            }
            i++;
        }
        if ( i == 0 )
        {
            out.println( "No indexes" );
        }
        else
        {
            printer.print( out );
        }
    }

    private String commaSeparate( Iterable<String> keys )
    {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for ( String key : keys )
        {
            if ( !first )
            {
                builder.append( ", " );

            }
            else
            {
                first = false;
            }
            builder.append( key );
        }
        return builder.toString();
    }

    private Iterable<IndexDefinition> indexesByLabelAndProperty( org.neo4j.graphdb.schema.Schema schema,
            Label[] labels, final String property )
    {
        Iterable<IndexDefinition> indexes = indexesByLabel( schema, labels );
        if ( property != null )
        {
            indexes = filter( index -> indexOf( property, index.getPropertyKeys() ) != -1, indexes );
        }
        return indexes;
    }

    private Iterable<ConstraintDefinition> constraintsByLabelAndProperty( org.neo4j.graphdb.schema.Schema schema,
            final Label[] labels, final String property )
    {
        return filter( constraint -> isNodeConstraint( constraint ) &&
                             hasLabel( constraint, labels ) &&
                             isMatchingConstraint( constraint, property ), schema.getConstraints() );
    }

    private Iterable<ConstraintDefinition> constraintsByTypeAndProperty( org.neo4j.graphdb.schema.Schema schema,
            final RelationshipType[] types, final String property )
    {
        return filter( constraint -> isRelationshipConstraint( constraint ) &&
                             hasType( constraint, types ) &&
                             isMatchingConstraint( constraint, property ), schema.getConstraints() );
    }

    private boolean hasLabel( ConstraintDefinition constraint, Label[] labels )
    {
        if ( labels.length == 0 )
        {
            return true;
        }

        for ( Label label : labels )
        {
            if ( constraint.getLabel().name().equals( label.name() ) )
            {
                return true;
            }
        }

        return false;
    }

    private static boolean hasType( ConstraintDefinition constraint, RelationshipType[] types )
    {
        if ( types.length == 0 )
        {
            return true;
        }

        for ( RelationshipType type : types )
        {
            if ( constraint.getRelationshipType().name().equals( type.name() ) )
            {
                return true;
            }
        }

        return false;
    }

    private static boolean isNodeConstraint( ConstraintDefinition constraint )
    {
        return constraint.isConstraintType( ConstraintType.UNIQUENESS ) ||
               constraint.isConstraintType( ConstraintType.NODE_KEY ) ||
               constraint.isConstraintType( ConstraintType.NODE_PROPERTY_EXISTENCE );
    }

    private static boolean isRelationshipConstraint( ConstraintDefinition constraint )
    {
        return constraint.isConstraintType( ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE );
    }

    private boolean isMatchingConstraint( ConstraintDefinition constraint, final String property )
    {
        return property == null || indexOf( property, constraint.getPropertyKeys() ) != -1;
    }

    private Iterable<IndexDefinition> indexesByLabel( org.neo4j.graphdb.schema.Schema schema, Label[] labels )
    {
        Iterable<IndexDefinition> indexes = schema.getIndexes();
        for ( final Label label : labels )
        {
            indexes = filter( item -> item.getLabel().name().equals( label.name() ), indexes );
        }
        return indexes;
    }

    private static String indent( String str )
    {
        return INDENT + str;
    }
}
