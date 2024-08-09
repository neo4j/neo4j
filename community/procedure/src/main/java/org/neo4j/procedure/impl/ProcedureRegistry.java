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
package org.neo4j.procedure.impl;

import static java.lang.String.format;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.neo4j.collection.ResourceRawIterator;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.PermissionState;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.procedure.UnsupportedDatabaseTypes;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.AnyValue;

public class ProcedureRegistry {

    private final ProcedureHolder<CallableProcedure> procedures;
    private final ProcedureHolder<CallableUserFunction> functions;
    private final ProcedureHolder<CallableUserAggregationFunction> aggregationFunctions;

    public ProcedureRegistry() {
        this(new ProcedureHolder<>(), new ProcedureHolder<>(), new ProcedureHolder<>());
    }

    private ProcedureRegistry(
            ProcedureHolder<CallableProcedure> procedures,
            ProcedureHolder<CallableUserFunction> functions,
            ProcedureHolder<CallableUserAggregationFunction> aggregationFunctions) {
        this.procedures = procedures;
        this.functions = functions;
        this.aggregationFunctions = aggregationFunctions;
    }

    /**
     * Register a new procedure.
     *
     * @param proc the procedure.
     */
    public void register(CallableProcedure proc) throws ProcedureException {
        ProcedureSignature signature = proc.signature();
        QualifiedName name = signature.name();

        String descriptiveName = signature.toString();
        validateSignature(descriptiveName, signature.inputSignature(), "input");
        validateSignature(descriptiveName, signature.outputSignature(), "output");

        if (!signature.isVoid() && signature.outputSignature().isEmpty()) {
            throw new ProcedureException(
                    Status.Procedure.ProcedureRegistrationFailed,
                    "Procedures with zero output fields must be declared as VOID");
        }

        var supportedScopes = signature.supportedCypherScopes();
        for (var scope : supportedScopes) {
            if (procedures.contains(name, scope)) {
                throw new ProcedureException(
                        Status.Procedure.ProcedureRegistrationFailed,
                        "Unable to register procedure, because the name `%s` is already in use.",
                        name);
            }
        }

        procedures.put(name, supportedScopes, proc, signature.caseInsensitive());
    }

    /**
     * Register a new function.
     *
     * @param function the function.
     */
    public void register(CallableUserFunction function) throws ProcedureException {
        UserFunctionSignature signature = function.signature();
        QualifiedName name = signature.name();
        var supportedScopes = signature.supportedCypherScopes();

        for (var scope : supportedScopes) {
            if (aggregationFunctions.contains(name, scope)) {
                throw new ProcedureException(
                        Status.Procedure.ProcedureRegistrationFailed,
                        "Unable to register function, because the name `%s` is already in use as an aggregation function.",
                        name);
            }

            if (functions.contains(name, scope)) {
                throw new ProcedureException(
                        Status.Procedure.ProcedureRegistrationFailed,
                        "Unable to register function, because the name `%s` is already in use.",
                        name);
            }
        }

        functions.put(name, supportedScopes, function, signature.caseInsensitive());
    }

    /**
     * Register a new function.
     *
     * @param function the function.
     */
    public void register(CallableUserAggregationFunction function) throws ProcedureException {
        UserFunctionSignature signature = function.signature();
        QualifiedName name = signature.name();
        var supportedScopes = signature.supportedCypherScopes();

        for (var scope : supportedScopes) {
            if (functions.contains(name, scope)) {
                throw new ProcedureException(
                        Status.Procedure.ProcedureRegistrationFailed,
                        "Unable to register aggregation function, because the name `%s` is already in use as a function.",
                        name);
            }

            if (aggregationFunctions.contains(name, scope)) {
                throw new ProcedureException(
                        Status.Procedure.ProcedureRegistrationFailed,
                        "Unable to register aggregation function, because the name `%s` is already in use.",
                        name);
            }
        }

        aggregationFunctions.put(name, supportedScopes, function, signature.caseInsensitive());
    }

    private void validateSignature(String descriptiveName, List<FieldSignature> fields, String fieldType)
            throws ProcedureException {
        Set<String> names = new HashSet<>();
        for (FieldSignature field : fields) {
            if (!names.add(field.name())) {
                throw new ProcedureException(
                        Status.Procedure.ProcedureRegistrationFailed,
                        "Procedure `%s` cannot be registered, because it contains a duplicated " + fieldType
                                + " field, '%s'. " + "You need to rename or remove one of the duplicate fields.",
                        descriptiveName,
                        field.name());
            }
        }
    }

    public ProcedureHandle procedure(QualifiedName name, CypherScope scope) throws ProcedureException {
        CallableProcedure proc = procedures.getByKey(name, scope);
        if (proc == null) {
            throw noSuchProcedure(name);
        }
        return new ProcedureHandle(proc.signature(), procedures.idOfKey(name, scope));
    }

    public UserFunctionHandle function(QualifiedName name, CypherScope scope) {
        CallableUserFunction func = functions.getByKey(name, scope);
        if (func == null) {
            return null;
        }
        return new UserFunctionHandle(func.signature(), functions.idOfKey(name, scope));
    }

    public UserFunctionHandle aggregationFunction(QualifiedName name, CypherScope scope) {
        CallableUserAggregationFunction func = aggregationFunctions.getByKey(name, scope);
        if (func == null) {
            return null;
        }
        return new UserFunctionHandle(func.signature(), aggregationFunctions.idOfKey(name, scope));
    }

    public ResourceRawIterator<AnyValue[], ProcedureException> callProcedure(
            Context ctx, int id, AnyValue[] input, ResourceMonitor resourceMonitor) throws ProcedureException {
        CallableProcedure proc;
        try {
            proc = procedures.getById(id);
            var permission = ctx.securityContext().allowExecuteAdminProcedure(id);
            if (proc.signature().admin() && !permission.allowsAccess()) {
                String errorDescriptor = (permission == PermissionState.EXPLICIT_DENY)
                        ? "is not allowed"
                        : "permission has not been granted";
                String message = format(
                        "Executing admin procedure '%s' %s for %s.",
                        proc.signature().name(),
                        errorDescriptor,
                        ctx.securityContext().description());
                ctx.dependencyResolver()
                        .resolveDependency(AbstractSecurityLog.class)
                        .error(ctx.securityContext(), message);
                throw new AuthorizationViolationException(message);
            }
            verifyDBType(ctx, proc);
        } catch (IndexOutOfBoundsException e) {
            throw noSuchProcedure(id);
        }
        return proc.apply(ctx, input, resourceMonitor);
    }

    private void verifyDBType(Context ctx, CallableProcedure proc) throws ProcedureException {
        if (Arrays.stream(proc.signature().unsupportedDbTypes())
                        .anyMatch(t -> t.equals(UnsupportedDatabaseTypes.DatabaseType.SPD))
                && ctx.kernelTransaction().isSPDTransaction()) {
            throw new ProcedureException(
                    Status.Statement.SyntaxError,
                    "Procedure '" + proc.signature().name() + "' is not supported in SPD.");
        }
    }

    public AnyValue callFunction(Context ctx, int functionId, AnyValue[] input) throws ProcedureException {
        CallableUserFunction func;
        try {
            func = functions.getById(functionId);
        } catch (IndexOutOfBoundsException e) {
            throw noSuchFunction(functionId);
        }
        return func.apply(ctx, input);
    }

    public UserAggregationReducer createAggregationFunction(Context ctx, int id) throws ProcedureException {
        try {
            CallableUserAggregationFunction func = aggregationFunctions.getById(id);
            return func.createReducer(ctx);
        } catch (IndexOutOfBoundsException e) {
            throw noSuchFunction(id);
        }
    }

    private ProcedureException noSuchProcedure(QualifiedName name) {
        return new ProcedureException(
                Status.Procedure.ProcedureNotFound,
                "There is no procedure with the name `%s` registered for this database instance. "
                        + "Please ensure you've spelled the procedure name correctly and that the "
                        + "procedure is properly deployed.",
                name);
    }

    private ProcedureException noSuchProcedure(int id) {
        return new ProcedureException(
                Status.Procedure.ProcedureNotFound,
                "There is no procedure with the internal id `%d` registered for this database instance.",
                id);
    }

    private ProcedureException noSuchFunction(int id) {
        return new ProcedureException(
                Status.Procedure.ProcedureNotFound,
                "There is no function with the internal id `%d` registered for this database instance.",
                id);
    }

    public Stream<ProcedureSignature> getAllProcedures(CypherScope scope) {
        return stream(procedures, CallableProcedure::signature, (signature) -> signature
                .supportedCypherScopes()
                .contains(scope));
    }

    int[] getIdsOfProceduresMatching(Predicate<CallableProcedure> predicate) {
        return getIdsOf(procedures, predicate);
    }

    public Stream<UserFunctionSignature> getAllNonAggregatingFunctions(CypherScope scope) {
        return stream(functions, CallableUserFunction::signature, (signature) -> signature
                .supportedCypherScopes()
                .contains(scope));
    }

    int[] getIdsOfFunctionsMatching(Predicate<CallableUserFunction> predicate) {
        return getIdsOf(functions, predicate);
    }

    public Stream<UserFunctionSignature> getAllAggregatingFunctions(CypherScope scope) {
        return stream(aggregationFunctions, CallableUserAggregationFunction::signature, (signature) -> signature
                .supportedCypherScopes()
                .contains(scope));
    }

    int[] getIdsOfAggregatingFunctionsMatching(Predicate<CallableUserAggregationFunction> predicate) {
        return getIdsOf(aggregationFunctions, predicate);
    }

    @VisibleForTesting
    public void unregister(QualifiedName name) {
        procedures.unregister(name);
        functions.unregister(name);
        aggregationFunctions.unregister(name);
    }

    /**
     * Create an immutable copy of the ProcedureRegistry
     *
     * @param ref The source {@link ProcedureRegistry} to copy.
     *
     * @return an immutable copy of the source
     **/
    public static ProcedureRegistry copyOf(ProcedureRegistry ref) {
        return new ProcedureRegistry(
                ProcedureHolder.copyOf(ref.procedures),
                ProcedureHolder.copyOf(ref.functions),
                ProcedureHolder.copyOf(ref.aggregationFunctions));
    }

    /**
     * Create an tomestoned copy of the ProcedureRegistry
     *
     * @param ref The source {@link ProcedureRegistry} to tombstone and copy.
     * @param which Which QualifiedNames should be filtered.
     *
     * @return a tombstoned copy.
     **/
    public static ProcedureRegistry tombstone(ProcedureRegistry ref, Predicate<QualifiedName> which) {
        return new ProcedureRegistry(
                ProcedureHolder.tombstone(ref.procedures, which),
                ProcedureHolder.tombstone(ref.functions, which),
                ProcedureHolder.tombstone(ref.aggregationFunctions, which));
    }

    private static <T> int[] getIdsOf(ProcedureHolder<T> holder, Predicate<T> predicate) {
        var lst = new IntArrayList();
        holder.forEach((i, v) -> {
            if (predicate.test(v)) {
                lst.add(i);
            }
        });
        return lst.toArray();
    }

    private static <T, F> Stream<F> stream(
            ProcedureHolder<T> holder, Function<T, F> transform, Predicate<F> condition) {
        Stream.Builder<F> builder = Stream.builder();
        holder.forEach((id, callable) -> {
            var value = transform.apply(callable);
            if (condition.test(value)) {
                builder.add(value);
            }
        });
        return builder.build();
    }
}
