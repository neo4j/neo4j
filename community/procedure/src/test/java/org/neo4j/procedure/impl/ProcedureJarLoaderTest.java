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
package org.neo4j.procedure.impl;

import org.hamcrest.core.IsEqual;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import java.util.zip.ZipException;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.jar.JarBuilder;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.Values;

import static java.util.stream.Collectors.toList;
import static org.apache.commons.text.StringEscapeUtils.escapeJava;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.api.ResourceManager.EMPTY_RESOURCE_MANAGER;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;

@SuppressWarnings( "WeakerAccess" )
@TestDirectoryExtension
public class ProcedureJarLoaderTest
{
    @Inject
    private TestDirectory testDirectory;

    private Log log = mock( Log.class );
    private final DependencyResolver dependencyResolver = new Dependencies();
    private final ValueMapper<Object> valueMapper = new DefaultValueMapper( mock( EmbeddedProxySPI.class ) );
    private final ProcedureJarLoader jarloader = new ProcedureJarLoader( new ProcedureCompiler( new TypeCheckers(), new ComponentRegistry(),
                    registryWithUnsafeAPI(), log, procedureConfig() ), NullLog.getInstance() );

    @Test
    void shouldLoadProcedureFromJar() throws Throwable
    {
        // Given
        URL jar = createJarFor( ClassWithOneProcedure.class );

        // When
        List<CallableProcedure> procedures = jarloader.loadProceduresFromDir( parentDir( jar ) ).procedures();

        // Then
        List<ProcedureSignature> signatures =
                procedures.stream().map( CallableProcedure::signature ).collect( toList() );
        assertThat( signatures, contains(
                procedureSignature( "org", "neo4j", "procedure", "impl", "myProcedure" )
                        .out( "someNumber", NTInteger ).build() ) );

        assertThat( asList( procedures.get( 0 ).apply( prepareContext(), new AnyValue[0], EMPTY_RESOURCE_MANAGER ) ),
                contains( IsEqual.equalTo( new AnyValue[]{Values.longValue( 1337L )} ) ) );
    }

    @Test
    void shouldLoadProcedureFromJarWithSpacesInFilename() throws Throwable
    {
        // Given
        URL jar = new JarBuilder().createJarFor( testDirectory.createFile( new Random().nextInt() + " some spaces in filename.jar" ),
                ClassWithOneProcedure.class);

        // When
        List<CallableProcedure> procedures = jarloader.loadProceduresFromDir( parentDir( jar ) ).procedures();

        // Then
        List<ProcedureSignature> signatures = procedures.stream().map( CallableProcedure::signature ).collect( toList() );
        assertThat( signatures,
                contains( procedureSignature( "org", "neo4j", "procedure", "impl", "myProcedure" ).out( "someNumber", NTInteger ).build() ) );

        assertThat( asList( procedures.get( 0 ).apply( prepareContext(), new AnyValue[0], EMPTY_RESOURCE_MANAGER ) ),
                contains( IsEqual.equalTo( new AnyValue[]{Values.longValue( 1337L )} ) ) );
    }

    @Test
    void shouldLoadProcedureWithArgumentFromJar() throws Throwable
    {
        // Given
        URL jar = createJarFor( ClassWithProcedureWithArgument.class );

        // When
        List<CallableProcedure> procedures = jarloader.loadProceduresFromDir( parentDir( jar ) ).procedures();

        // Then
        List<ProcedureSignature> signatures = procedures.stream().map( CallableProcedure::signature ).collect( toList() );
        assertThat( signatures, contains(
                procedureSignature( "org","neo4j", "procedure", "impl", "myProcedure" )
                        .in( "value", NTInteger )
                        .out( "someNumber", NTInteger )
                        .build() ));

        assertThat( asList( procedures.get( 0 )
                        .apply( prepareContext(), new AnyValue[]{Values.longValue( 42 )}, EMPTY_RESOURCE_MANAGER ) ),
                contains( IsEqual.equalTo( new AnyValue[]{Values.longValue( 42 )} ) ) );
    }

    @Test
    void shouldLoadProcedureFromJarWithMultipleProcedureClasses() throws Throwable
    {
        // Given
        URL jar = createJarFor( ClassWithOneProcedure.class, ClassWithAnotherProcedure.class, ClassWithNoProcedureAtAll.class );

        // When
        List<CallableProcedure> procedures = jarloader.loadProceduresFromDir( parentDir( jar ) ).procedures();

        // Then
        List<ProcedureSignature> signatures = procedures.stream().map( CallableProcedure::signature ).collect( toList() );
        assertThat( signatures, containsInAnyOrder(
                procedureSignature( "org","neo4j", "procedure", "impl", "myOtherProcedure" ).out( "someNumber", NTInteger ).build(),
                procedureSignature( "org","neo4j", "procedure", "impl", "myProcedure" ).out( "someNumber", NTInteger ).build() ));
    }

    @Test
    void shouldGiveHelpfulErrorOnInvalidProcedure() throws Throwable
    {
        // Given
        URL jar = createJarFor( ClassWithOneProcedure.class, ClassWithInvalidProcedure.class );

        ProcedureException exception = assertThrows( ProcedureException.class, () -> jarloader.loadProceduresFromDir( parentDir( jar ) ) );
        assertThat( exception.getMessage(), equalTo( String.format( "Procedures must return a Stream of records, where a record is a concrete class%n" +
                "that you define, with public non-final fields defining the fields in the record.%n" +
                "If you''d like your procedure to return `boolean`, you could define a record class " +
                "like:%n" +
                "public class Output '{'%n" +
                "    public boolean out;%n" +
                "'}'%n" +
                "%n" +
                "And then define your procedure as returning `Stream<Output>`." )) );
    }

    @Test
    void shouldLoadProceduresFromDirectory() throws Throwable
    {
        // Given
        createJarFor( ClassWithOneProcedure.class );
        createJarFor( ClassWithAnotherProcedure.class );

        // When
        List<CallableProcedure> procedures = jarloader.loadProceduresFromDir( testDirectory.directory() ).procedures();

        // Then
        List<ProcedureSignature> signatures = procedures.stream().map( CallableProcedure::signature ).collect( toList() );
        assertThat( signatures, containsInAnyOrder(
                procedureSignature( "org","neo4j", "procedure", "impl", "myOtherProcedure" ).out( "someNumber", NTInteger ).build(),
                procedureSignature( "org","neo4j", "procedure", "impl", "myProcedure" ).out( "someNumber", NTInteger ).build() ));
    }

    @Test
    void shouldGiveHelpfulErrorOnWildCardProcedure() throws Throwable
    {
        // Given
        URL jar = createJarFor( ClassWithWildCardStream.class );

        ProcedureException exception = assertThrows( ProcedureException.class, () -> jarloader.loadProceduresFromDir( parentDir( jar ) ) );
        assertThat( exception.getMessage(), equalTo( String.format( "Procedures must return a Stream of records, where a record is a concrete class%n" +
                "that you define and not a Stream<?>." ) ) );
    }

    @Test
    void shouldGiveHelpfulErrorOnRawStreamProcedure() throws Throwable
    {
        // Given
        URL jar = createJarFor( ClassWithRawStream.class );

        ProcedureException exception = assertThrows( ProcedureException.class, () -> jarloader.loadProceduresFromDir( parentDir( jar ) ) );
        assertThat( exception.getMessage(), equalTo( String.format( "Procedures must return a Stream of records, where a record is a concrete class%n" +
                "that you define and not a raw Stream." ) ) );
    }

    @Test
    void shouldGiveHelpfulErrorOnGenericStreamProcedure() throws Throwable
    {
        // Given
        URL jar = createJarFor( ClassWithGenericStream.class );

        ProcedureException exception = assertThrows( ProcedureException.class, () -> jarloader.loadProceduresFromDir( parentDir( jar ) ) );
        assertThat( exception.getMessage(), equalTo( String.format( "Procedures must return a Stream of records, where a record is a concrete class%n" +
                "that you define and not a parameterized type such as java.util.List<org.neo4j" +
                ".procedure.impl.ProcedureJarLoaderTest$Output>.") ) );
    }

    @Test
    void shouldLogHelpfullyWhenPluginJarIsCorrupt() throws Exception
    {
        // given
        URL theJar = createJarFor( ClassWithOneProcedure.class, ClassWithAnotherProcedure.class, ClassWithNoProcedureAtAll.class );
        corruptJar( theJar );

        AssertableLogProvider logProvider = new AssertableLogProvider( true );

        ProcedureJarLoader jarloader = new ProcedureJarLoader(
                new ProcedureCompiler( new TypeCheckers(), new ComponentRegistry(), registryWithUnsafeAPI(), log, procedureConfig() ),
                logProvider.getLog( ProcedureJarLoader.class ) );

        // when
        assertThrows( ZipException.class, () -> jarloader.loadProceduresFromDir( parentDir( theJar ) ) );
        logProvider.internalToStringMessageMatcher().assertContains(
                escapeJava( String.format( "Plugin jar file: %s corrupted.", new File( theJar.toURI() ).toPath() ) ) );
    }

    @Test
    void shouldWorkOnPathsWithSpaces() throws Exception
    {
        // given
        File fileWithSpacesInName = testDirectory.createFile( new Random().nextInt() + "  some spaces in the filename" + ".jar" );
        URL theJar = new JarBuilder().createJarFor( fileWithSpacesInName, ClassWithOneProcedure.class );
        corruptJar( theJar );

        AssertableLogProvider logProvider = new AssertableLogProvider( true );

        ProcedureJarLoader jarloader = new ProcedureJarLoader(
                new ProcedureCompiler( new TypeCheckers(), new ComponentRegistry(), registryWithUnsafeAPI(), log, procedureConfig() ),
                logProvider.getLog( ProcedureJarLoader.class ) );

        // when
        assertThrows( ZipException.class, () -> jarloader.loadProceduresFromDir( parentDir( theJar ) ) );
        logProvider.internalToStringMessageMatcher().assertContains(
                escapeJava( String.format( "Plugin jar file: %s corrupted.", fileWithSpacesInName.toPath() ) ) );
    }

    @Test
    void shouldReturnEmptySetOnNullArgument() throws Exception
    {
        // given
        ProcedureJarLoader jarloader = new ProcedureJarLoader(
                new ProcedureCompiler( new TypeCheckers(), new ComponentRegistry(), registryWithUnsafeAPI(), log, procedureConfig() ),
                NullLog.getInstance() );

        // when
        ProcedureJarLoader.Callables callables = jarloader.loadProceduresFromDir( null );

        // then
        assertEquals( 0, callables.procedures().size() + callables.functions().size() );
    }

    private org.neo4j.kernel.api.procedure.Context prepareContext()
    {
        return buildContext( dependencyResolver, valueMapper ).context();
    }

    private File parentDir( URL jar ) throws URISyntaxException
    {
        return new File( jar.toURI() ).getParentFile();
    }

    private void corruptJar( URL jar ) throws IOException, URISyntaxException
    {
        File jarFile = new File( jar.toURI() ).getCanonicalFile();
        long fileLength = jarFile.length();
        byte[] bytes = Files.readAllBytes( Paths.get( jar.toURI() ) );
        for ( long i = fileLength / 2; i < fileLength; i++ )
        {
            bytes[(int) i] = 0;
        }
        Files.write( jarFile.toPath(), bytes );
    }

    private URL createJarFor( Class<?> ... targets ) throws IOException
    {
        return new JarBuilder().createJarFor( testDirectory.createFile( new Random().nextInt() + ".jar" ), targets );
    }

    public static class Output
    {
        public long someNumber = 1337; // Public because needed by a mapper

        public Output()
        {

        }

        public Output( long anotherNumber )
        {
            this.someNumber = anotherNumber;
        }
    }

    public static class ClassWithInvalidProcedure
    {
        @Procedure
        public boolean booleansAreNotAcceptableReturnTypes()
        {
            return false;
        }
    }

    public static class ClassWithOneProcedure
    {
        @Procedure
        public Stream<Output> myProcedure()
        {
            return Stream.of( new Output() );
        }
    }

    public static class ClassWithNoProcedureAtAll
    {
        void thisMethodIsEntirelyUnrelatedToAllThisExcitement()
        {

        }
    }

    public static class ClassWithAnotherProcedure
    {
        @Procedure
        public Stream<Output> myOtherProcedure()
        {
            return Stream.of( new Output() );
        }
    }

    public static class ClassWithProcedureWithArgument
    {
        @Procedure
        public Stream<Output> myProcedure( @Name( "value" ) long value )
        {
            return Stream.of( new Output( value ) );
        }
    }

    public static class ClassWithWildCardStream
    {
        @Procedure
        public Stream<?> wildCardProc()
        {
            return Stream.of( new Output() );
        }
    }

    public static class ClassWithRawStream
    {
        @Procedure
        public Stream rawStreamProc()
        {
            return Stream.of( new Output() );
        }
    }

    public static class ClassWithGenericStream
    {
        @Procedure
        public Stream<List<Output>> genericStream()
        {
            return Stream.of( Collections.singletonList( new Output() ));
        }
    }

    public static class ClassWithUnsafeComponent
    {
        @Context
        public UnsafeAPI api;

        @Procedure
        public Stream<Output> unsafeProcedure()
        {
            return Stream.of( new Output( api.getNumber() ) );
        }

        @UserFunction
        public long unsafeFunction()
        {
            return api.getNumber();
        }
    }

    public static class ClassWithUnsafeConfiguredComponent
    {
        @Context
        public UnsafeAPI api;

        @Procedure
        public Stream<Output> unsafeFullAccessProcedure()
        {
            return Stream.of( new Output( api.getNumber() ) );
        }

        @UserFunction
        public long unsafeFullAccessFunction()
        {
            return api.getNumber();
        }
    }

    private static class UnsafeAPI
    {
        public long getNumber()
        {
            return 7331;
        }
    }

    private ComponentRegistry registryWithUnsafeAPI()
    {
        ComponentRegistry allComponents = new ComponentRegistry();
        allComponents.register( UnsafeAPI.class, ctx -> new UnsafeAPI() );
        return allComponents;
    }

    private ProcedureConfig procedureConfig()
    {
        Config config = Config.defaults( procedure_unrestricted, List.of( "org.neo4j.kernel.impl.proc.unsafeFullAccess*" ) );
        return new ProcedureConfig( config );
    }
}
