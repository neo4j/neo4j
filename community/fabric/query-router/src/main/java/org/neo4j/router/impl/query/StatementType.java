package org.neo4j.router.impl.query;

import org.neo4j.cypher.internal.ast.AdministrationCommand;
import org.neo4j.cypher.internal.ast.Query;
import org.neo4j.cypher.internal.ast.SchemaCommand;
import org.neo4j.cypher.internal.ast.Statement;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.planning.QueryType;
import org.neo4j.kernel.api.exceptions.Status;

public enum StatementType {
    READ_QUERY(true, true, false, "Read query"),
    READ_PLUS_UNRESOLVED_QUERY(true, false, false, "Read query (with unresolved procedures)"),
    WRITE_QUERY(true, false, false, "Write query"),
    SCHEMA_COMMAND(false, false, true, "Schema modification"),
    OTHER(false, false, false, "Administration command");

    private final boolean isQuery;
    private final boolean isReadQuery;
    private final boolean isSchemaCommand;
    private final String toString;

    StatementType(boolean isQuery, boolean isReadQuery, boolean isSchemaCommand, String toString) {
        this.isQuery = isQuery;
        this.isReadQuery = isReadQuery;
        this.isSchemaCommand = isSchemaCommand;
        this.toString = toString;
    }

    // Java access helpers
    public boolean isQuery() {
        return isQuery;
    }

    public boolean isReadQuery() {
        return isReadQuery;
    }

    public boolean isSchemaCommand() {
        return isSchemaCommand;
    }

    public static StatementType of(Statement statement) {
        if (statement instanceof Query) {
            var queryType = QueryType.of(statement);
            if (queryType == org.neo4j.fabric.planning.QueryType.READ_PLUS_UNRESOLVED()) {
                return READ_PLUS_UNRESOLVED_QUERY;
            }

            return queryType.isRead() ? READ_QUERY : WRITE_QUERY;
        } else if (statement instanceof SchemaCommand) {
            return SCHEMA_COMMAND;
        } else if (statement instanceof AdministrationCommand) {
            return OTHER;
        } else {
            // Should never get here
            throw new FabricException(
                    Status.Statement.ExecutionFailed,
                    "Statement `" + statement.asCanonicalStringVal() + "` not recognized.");
        }
    }

    @Override
    public String toString() {
        return toString;
    }
}
