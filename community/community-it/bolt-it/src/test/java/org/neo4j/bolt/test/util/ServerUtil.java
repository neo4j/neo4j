/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.test.util;

import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;

import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.collection.ResourceRawIterator;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.IntegralValue;

public final class ServerUtil {
    private ServerUtil() {}

    /**
     * Retrieves teh globally unique identifier assigned to the home database.
     *
     * @param server a server instance.
     * @return a database identifier.
     */
    public static NamedDatabaseId getDatabaseId(Neo4jWithSocket server) {
        var resolver = ((GraphDatabaseAPI) server.graphDatabaseService()).getDependencyResolver();
        var database = resolver.resolveDependency(Database.class);
        return database.getNamedDatabaseId();
    }

    /**
     * Retrieves the globally unique identifier of the last comitted transaction.
     *
     * @param server a server instance.
     * @return a transaction identifier.
     */
    public static long getLastClosedTransactionId(Neo4jWithSocket server) {
        var resolver = ((GraphDatabaseAPI) server.graphDatabaseService()).getDependencyResolver();
        var txIdStore = resolver.resolveDependency(TransactionIdStore.class);
        return txIdStore.getLastClosedTransactionId();
    }

    public static <T> T resolveDependency(Neo4jWithSocket server, Class<T> type) {
        var dbApi = (GraphDatabaseAPI) server.graphDatabaseService();
        return dbApi.getDependencyResolver().resolveDependency(type);
    }

    private static GlobalProcedures getProcedures(Neo4jWithSocket server) {
        return resolveDependency(server, GlobalProcedures.class);
    }

    public static <T> void registerComponent(
            Neo4jWithSocket server, Class<T> type, ThrowingFunction<Context, T, ProcedureException> provider) {
        getProcedures(server).registerComponent(type, provider, true);
    }

    public static void installProcedure(Neo4jWithSocket server, CallableProcedure procedure) throws ProcedureException {
        getProcedures(server).register(procedure);
    }

    public static void installProcedure(Neo4jWithSocket server, Class<?> procedure) throws KernelException {
        getProcedures(server).registerProcedure(procedure);
    }

    public static void installSleepProcedure(Neo4jWithSocket server) throws ProcedureException {
        installProcedure(
                server,
                new CallableProcedure.BasicProcedure(procedureSignature(new QualifiedName("boltissue", "sleep"))
                        .in("data", Neo4jTypes.NTInteger)
                        .out(ProcedureSignature.VOID)
                        .build()) {
                    @Override
                    public ResourceRawIterator<AnyValue[], ProcedureException> apply(
                            Context context, AnyValue[] objects, ResourceMonitor resourceMonitor)
                            throws ProcedureException {
                        try {
                            Thread.sleep(((IntegralValue) objects[0]).longValue());
                        } catch (InterruptedException e) {
                            throw new ProcedureException(Status.General.UnknownError, e, "Interrupted");
                        }
                        return ResourceRawIterator.empty();
                    }
                });
    }
}
