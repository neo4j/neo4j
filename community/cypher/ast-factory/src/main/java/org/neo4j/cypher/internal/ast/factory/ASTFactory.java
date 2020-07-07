/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.ast.factory;

import java.util.List;

/**
 * Factory for constructing ASTs.
 * <p>
 * This interface is generic in many dimensions, in order to support type-safe construction of ASTs
 * without depending on the concrete AST type. This architecture allows code which creates/manipulates AST
 * to live independently of the AST, and thus makes sharing and reuse of these components much easier.
 * <p>
 * The factory contains methods for creating AST representing all of Cypher 9, as defined
 * at `https://github.com/opencypher/openCypher/`, and implemented in `https://github.com/opencypher/front-end`.
 * <p>
 * Schema commands like `CREATE/DROP INDEX` as not supported, nor system DSL used in Neo4j.
 *
 * @param <POS> type used to mark the input position of the created AST node.
 */
public interface ASTFactory<STATEMENT,
        QUERY extends STATEMENT,
        CLAUSE,
        RETURN_CLAUSE extends CLAUSE,
        RETURN_ITEM,
        ORDER_ITEM,
        PATTERN,
        NODE_PATTERN,
        REL_PATTERN,
        PATH_LENGTH,
        SET_CLAUSE extends CLAUSE,
        SET_ITEM,
        REMOVE_ITEM,
        CALL_RESULT_ITEM,
        HINT,
        EXPRESSION,
        VARIABLE extends EXPRESSION,
        PROPERTY extends EXPRESSION,
        MAP_PROJECTION_ITEM,
        POS>
        extends ASTExpressionFactory<EXPRESSION,PATTERN,VARIABLE,PROPERTY,MAP_PROJECTION_ITEM,POS>
{
    final class NULL
    {
        private NULL()
        {
            throw new IllegalStateException( "This class should not be instantiated, use `null` instead." );
        }
    }

    class StringPos<POS>
    {
        public final String string;
        public final POS pos;

        public StringPos( String string, POS pos )
        {
            this.string = string;
            this.pos = pos;
        }
    }

    // QUERY

    QUERY newSingleQuery( List<CLAUSE> clauses );

    QUERY newUnion( POS p, QUERY lhs, QUERY rhs, boolean all );

    QUERY periodicCommitQuery( POS p, String batchSize, CLAUSE loadCsv, List<CLAUSE> queryBody );

    CLAUSE fromClause( POS p, EXPRESSION e );

    CLAUSE useClause( POS p, EXPRESSION e );

    RETURN_CLAUSE newReturnClause( POS p, boolean distinct,
                                   boolean returnAll,
                                   List<RETURN_ITEM> returnItems,
                                   List<ORDER_ITEM> order,
                                   EXPRESSION skip,
                                   EXPRESSION limit );

    RETURN_CLAUSE newReturnGraphClause( POS p );

    RETURN_ITEM newReturnItem( POS p, EXPRESSION e, VARIABLE v );

    RETURN_ITEM newReturnItem( POS p, EXPRESSION e, int eStartOffset, int eEndOffset );

    ORDER_ITEM orderDesc( EXPRESSION e );

    ORDER_ITEM orderAsc( EXPRESSION e );

    CLAUSE withClause( POS p, RETURN_CLAUSE returnClause, EXPRESSION where );

    CLAUSE matchClause( POS p, boolean optional, List<PATTERN> patterns, List<HINT> hints, EXPRESSION where );

    HINT usingIndexHint( POS p, VARIABLE v, String label, List<String> properties, boolean seekOnly );

    HINT usingJoin( POS p, List<VARIABLE> joinVariables );

    HINT usingScan( POS p, VARIABLE v, String label );

    CLAUSE createClause( POS p, List<PATTERN> patterns );

    SET_CLAUSE setClause( POS p, List<SET_ITEM> setItems );

    SET_ITEM setProperty( PROPERTY property, EXPRESSION value );

    SET_ITEM setVariable( VARIABLE variable, EXPRESSION value );

    SET_ITEM addAndSetVariable( VARIABLE variable, EXPRESSION value );

    SET_ITEM setLabels( VARIABLE variable, List<StringPos<POS>> value );

    CLAUSE removeClause( POS p, List<REMOVE_ITEM> removeItems );

    REMOVE_ITEM removeProperty( PROPERTY property );

    REMOVE_ITEM removeLabels( VARIABLE variable, List<StringPos<POS>> labels );

    CLAUSE deleteClause( POS p, boolean detach, List<EXPRESSION> expressions );

    CLAUSE unwindClause( POS p, EXPRESSION e, VARIABLE v );

    enum MergeActionType
    {
        OnCreate,
        OnMatch
    }

    CLAUSE  mergeClause( POS p, PATTERN pattern, List<SET_CLAUSE> setClauses, List<MergeActionType> actionTypes );

    CLAUSE callClause( POS p,
                       List<String> namespace,
                       String name,
                       List<EXPRESSION> arguments,
                       List<CALL_RESULT_ITEM> resultItems,
                       EXPRESSION where );

    CALL_RESULT_ITEM callResultItem( POS p, String name, VARIABLE v );

    PATTERN namedPattern( VARIABLE v, PATTERN pattern );

    PATTERN shortestPathPattern( POS p, PATTERN pattern );

    PATTERN allShortestPathsPattern( POS p, PATTERN pattern );

    PATTERN everyPathPattern( List<NODE_PATTERN> nodes, List<REL_PATTERN> relationships );

    NODE_PATTERN nodePattern( POS p, VARIABLE v, List<StringPos<POS>> labels, EXPRESSION properties );

    REL_PATTERN relationshipPattern( POS p,
                                     boolean left,
                                     boolean right,
                                     VARIABLE v,
                                     List<StringPos<POS>> relTypes,
                                     PATH_LENGTH pathLength,
                                     EXPRESSION properties,
                                     boolean legacyTypeSeparator );

    /**
     * Create a path-length object used to specify path lengths for variable length patterns.
     *
     * Note that paths will be reported in a quite specific manner:
     *     Cypher       minLength   maxLength
     *     ----------------------------------
     *     [*]          null        null
     *     [*2]         "2"         "2"
     *     [*2..]       "2"         ""
     *     [*..3]       ""          "3"
     *     [*2..3]      "2"         "3"
     *     [*..]        ""          ""      <- separate from [*] to allow specific error messages
     */
    PATH_LENGTH pathLength( POS p, String minLength, String maxLength );

    CLAUSE loadCsvClause( POS p, boolean headers, EXPRESSION source, VARIABLE v, String fieldTerminator );

    CLAUSE foreachClause( POS p, VARIABLE v, EXPRESSION list, List<CLAUSE> clauses );

    CLAUSE subqueryClause( POS p, QUERY subquery );
}
