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
package org.neo4j.tooling.procedure;

import com.google.auto.service.AutoService;
import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.messages.MessagePrinter;
import org.neo4j.tooling.procedure.visitors.PerformsWriteMethodVisitor;

import java.lang.annotation.Annotation;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.TypeElement;

import org.neo4j.procedure.PerformsWrites;

@AutoService( Processor.class )
public class PerformsWriteProcessor extends AbstractProcessor
{
    private static final Class<? extends Annotation> performWritesType = PerformsWrites.class;
    private MessagePrinter messagePrinter;
    private ElementVisitor<Stream<CompilationMessage>,Void> visitor;

    @Override
    public Set<String> getSupportedAnnotationTypes()
    {
        Set<String> types = new HashSet<>();
        types.add( performWritesType.getName() );
        return types;
    }

    @Override
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.RELEASE_8;
    }

    @Override
    public synchronized void init( ProcessingEnvironment processingEnv )
    {
        super.init( processingEnv );
        messagePrinter = new MessagePrinter( processingEnv.getMessager() );
        visitor = new PerformsWriteMethodVisitor();
    }

    @Override
    public boolean process( Set<? extends TypeElement> annotations, RoundEnvironment roundEnv )
    {
        processPerformsWriteElements( roundEnv );
        return false;
    }

    private void processPerformsWriteElements( RoundEnvironment roundEnv )
    {
        roundEnv.getElementsAnnotatedWith( performWritesType ).stream().flatMap( this::validate )
                .forEachOrdered( messagePrinter::print );

    }

    private Stream<CompilationMessage> validate( Element element )
    {
        return visitor.visit( element );
    }
}
