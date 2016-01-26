/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.proc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.proc.Procedure;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.builtinprocs.BuiltInProcedures;
import org.neo4j.kernel.configuration.AsciiDocItem;
import org.neo4j.kernel.configuration.AsciiDocListGenerator;

import static java.util.stream.Collectors.toList;

/** Produces asciidoc documentation from procedures. */
public class ProcedureAsciiDocGenerator
{
    public static void main(String ... args) throws KernelException, IOException
    {
        // Default - generate for standard builtins
        File output = null;
        if(args.length > 0)
        {
            output = new File(args[0]).getAbsoluteFile();
        }

        String doc = new ProcedureAsciiDocGenerator()
                .generateDocsFor( "builtin_procedures", "Built in procedures", BuiltInProcedures.BUILTINS);

        if(output != null)
        {
            System.out.println("Saving docs for built-in procedures in '" + output.getAbsolutePath() + "'.");
            FileUtils.writeToFile(output, doc, false);
        } else
        {
            System.out.println(doc);
        }
    }
    public String generateDocsFor( String tableId, String tableTitle, Class ... procedureClasses ) throws KernelException
    {
        StringBuilder details = new StringBuilder();
        List<AsciiDocItem> items =
            signatures( procedureClasses )
                .map( ( sig ) -> {
                    describeProcedure( sig, details );
                    return sig;
                })
                .map( ( sig ) -> new AsciiDocItem(
                        procedureId( sig ),
                        sig.name().toString(),
                        sig.description() ) )
                .collect( toList() );

        String table = new AsciiDocListGenerator( tableId, tableTitle, true ).generateListAndTableCombo( items );
        return table + details.toString();
    }

    private String procedureId( ProcedureSignature sig )
    {
        return "builtinproc_" + sig.name().toString().replace( ".", "_" );
    }

    private void describeProcedure( ProcedureSignature sig, StringBuilder out )
    {
        // This is a small table generated for each procedure, which contains the
        // full description and the procedure signature.
        out.append( "[[" ).append( procedureId( sig ) ).append( "]]\n" );
        out.append( "." ).append( sig.name().toString() ).append( "\n" );
        out.append( "[cols=\"<1h,<4\"]\n" );
        out.append( "|===\n" );
        out.append( "|Signature |").append( sig.toString() ).append( "\n" );
        out.append( "|Description |").append( sig.description() ).append( "\n" );
        out.append( "|===\n\n" );
    }

    private Stream<ProcedureSignature> signatures( Class[] procedureClasses ) throws KernelException
    {
        TypeMappers types = new TypeMappers();
        ComponentRegistry components = new ComponentRegistry();

        // For the purposes of generating documentation, register a fallback injection provider
        // that pretends any component is fine for injection, so we don't have to emulate all
        // the real injectors used at runtime.
        components.registerFallback( (cls) -> ((ctx) -> null) );

        ReflectiveProcedureCompiler compiler = new ReflectiveProcedureCompiler( types, components );

        List<ProcedureSignature> signatures = new ArrayList<>();
        for ( Class procedureClass : procedureClasses )
        {
            signatures.addAll( compiler
                    .compile( procedureClass )
                    .stream().map( Procedure::signature )
                    .collect( toList()) );
        }

        return signatures.stream()
                .sorted( (a,b) -> a.toString().compareTo( b.toString() ) );
    }
}
