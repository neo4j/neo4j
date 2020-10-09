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
package org.neo4j.annotations.api;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;
import javax.lang.model.type.WildcardType;
import javax.lang.model.util.Types;
import javax.tools.FileObject;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static javax.tools.Diagnostic.Kind.ERROR;
import static javax.tools.Diagnostic.Kind.NOTE;
import static javax.tools.Diagnostic.Kind.WARNING;
import static javax.tools.StandardLocation.CLASS_OUTPUT;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;
import static org.neo4j.annotations.AnnotationConstants.DEFAULT_NEW_LINE;
import static org.neo4j.annotations.AnnotationConstants.WINDOWS_NEW_LINE;

/**
 * Generates public API signatures from all the classes marked with {@link PublicApi}. It performs some sanity checking so that all exposed types are visible.
 */
public class PublicApiAnnotationProcessor extends AbstractProcessor
{
    /**
     * This should be enabled from the build system
     */
    static final String VERIFY_TOGGLE = "enablePublicApiSignatureCheck";

    private final Set<String> publicElements = new TreeSet<>();
    private final Set<String> validatedDeclaredTypes = new HashSet<>();
    private final List<String> scope = new ArrayList<>();

    /**
     * Where to place the generated signature
     */
    static final String GENERATED_SIGNATURE_DESTINATION = "META-INF/PublicApi.txt";

    private final boolean testExecution;
    private final String newLine;
    private boolean inDeprecatedScope;
    private Types typeUtils;

    @SuppressWarnings( "unused" )
    public PublicApiAnnotationProcessor()
    {
        this( false );
    }

    /**
     * Used from tests since the in-memory filesystem there does not support all of the needed operations.
     * Welcome to the world of impossible-to-test annotation processors!
     */
    PublicApiAnnotationProcessor( boolean forTest )
    {
        this( forTest, DEFAULT_NEW_LINE );
    }

    /**
     * Used from tests since the in-memory filesystem there does not support all of the needed operations.
     * Welcome to the world of impossible-to-test annotation processors!
     */
    PublicApiAnnotationProcessor( boolean forTest, String newLine )
    {
        this.testExecution = forTest;
        this.newLine = newLine;
    }

    @Override
    public synchronized void init( ProcessingEnvironment processingEnv )
    {
        super.init( processingEnv );
        typeUtils = processingEnv.getTypeUtils();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes()
    {
        return Set.of( PublicApi.class.getName() );
    }

    @Override
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.latestSupported();
    }

    @Override
    public boolean process( Set<? extends TypeElement> annotations, RoundEnvironment roundEnv )
    {
        try
        {
            if ( roundEnv.processingOver() )
            {
                if ( !roundEnv.errorRaised() )
                {
                    generateSignature();
                }
            }
            else
            {
                process( roundEnv );
            }
        }
        catch ( Exception e )
        {
            error( "Public API annotation processor failed: " + getStackTrace( e ) );
        }
        return false;
    }

    private void generateSignature() throws IOException
    {
        // only verify on request
        if ( !Boolean.getBoolean( VERIFY_TOGGLE ) )
        {
            return;
        }

        if ( !publicElements.isEmpty() )
        {
            StringBuilder sb = new StringBuilder();
            for ( final String element : publicElements )
            {
                sb.append( element ).append( newLine );
            }
            String newSignature = sb.toString();

            // Write new signature
            final FileObject file = processingEnv.getFiler().createResource( CLASS_OUTPUT, "", GENERATED_SIGNATURE_DESTINATION );
            try ( BufferedWriter writer = new BufferedWriter( file.openWriter() ) )
            {
                writer.write( newSignature );
            }

            if ( !testExecution )
            {
                // Verify files
                Path path = Path.of( file.toUri() );
                Path metaPath = getAndAssertParent( path, "META-INF" );
                Path classesPath = getAndAssertParent( metaPath, "classes" );
                Path targetPath = getAndAssertParent( classesPath, "target" );
                Path mavenModulePath = targetPath.getParent();
                Path oldSignaturePath = mavenModulePath.resolve( "PublicApi.txt" );

                if ( Boolean.getBoolean( "overwrite" ) )
                {
                    info( "Overwriting " + oldSignaturePath );
                    Files.writeString( oldSignaturePath, newSignature, UTF_8, WRITE, CREATE, TRUNCATE_EXISTING );
                }

                if ( !Files.exists( oldSignaturePath ) )
                {
                    error( format( "Missing file %s, use `-Doverwrite` to create it.", oldSignaturePath ) );
                    return;
                }

                String oldSignature = Files.readString( oldSignaturePath, UTF_8 );
                if ( !oldSignature.equals( newSignature ) )
                {
                    oldSignature = oldSignature.replace( WINDOWS_NEW_LINE, DEFAULT_NEW_LINE );
                    newSignature = newSignature.replace( WINDOWS_NEW_LINE, DEFAULT_NEW_LINE );
                    if ( !oldSignature.equals( newSignature ) )
                    {
                        StringBuilder diff = diff( oldSignaturePath );
                        error( format( "Public API signature mismatch. The generated signature, %s, does not match the old signature in %s.%n" +
                                "Specify `-Doverwrite` to maven to replace it. Changed public elements, compared to the committed PublicApi.txt:%n%s%n",
                                path, oldSignaturePath, diff ) );
                    }
                }
                else
                {
                    info( "Public API signature matches. " + oldSignaturePath );
                }
            }
        }
    }

    private StringBuilder diff( Path oldSignaturePath ) throws IOException
    {
        Set<String> oldLines = new HashSet<>();
        try ( Stream<String> lines = Files.lines( oldSignaturePath, UTF_8 ) )
        {
            lines.forEach( oldLines::add );
        }
        StringBuilder diff = new StringBuilder();
        diffSide( diff, oldLines, publicElements, '-' );
        diffSide( diff, publicElements, oldLines, '+' );
        return diff;
    }

    private void diffSide( StringBuilder diff, Set<String> left, Set<String> right, char diffSign )
    {
        for ( String oldPublicElement : left )
        {
            if ( !right.contains( oldPublicElement ) )
            {
                diff.append( diffSign ).append( oldPublicElement ).append( format( "%n" ) );
            }
        }
    }

    private static Path getAndAssertParent( Path path, String name )
    {
        Path parent = path.getParent();
        if ( !parent.getFileName().toString().equals( name ) )
        {
            throw new IllegalStateException( path.toAbsolutePath() + " parent is not " + name );
        }
        return parent;
    }

    private void process( RoundEnvironment roundEnv )
    {
        final Set<TypeElement> elements = roundEnv.getElementsAnnotatedWith( PublicApi.class ).stream().map( TypeElement.class::cast ).collect( toSet() );
        for ( TypeElement publicClass : elements )
        {
            pushScope( publicClass.getQualifiedName().toString() );
            processType( publicClass );
            popScope();
        }
    }

    /**
     * Processing type elements. Class, interface, enum.
     */
    private void processType( TypeElement typeElement )
    {
        // Dummy check for public modifier
        if ( !typeElement.getModifiers().contains( Modifier.PUBLIC ) )
        {
            error( "Class marked as public is not actually public", typeElement );
        }

        // Add self to public API
        StringBuilder sb = new StringBuilder();
        addTypeName( sb, typeElement );
        addModifiers( sb, typeElement );
        addKindIdentifier( sb, typeElement );
        addSuperClass( sb, typeElement );
        addInterfaces( sb, typeElement );

        publicElements.add( sb.toString() );

        // Traverse visible child elements
        for ( Element element : typeElement.getEnclosedElements() )
        {
            Set<Modifier> modifiers = element.getModifiers();
            if ( modifiers.contains( Modifier.PUBLIC ) || modifiers.contains( Modifier.PROTECTED ) )
            {
                ElementKind kind = element.getKind();
                switch ( kind )
                {
                case ENUM:
                case INTERFACE:
                case CLASS:
                    pushScope( "." + element.getSimpleName() );
                    processType( (TypeElement) element );
                    break;
                case ENUM_CONSTANT:
                case FIELD:
                    pushScope( "#" + element );
                    processField( (VariableElement) element );
                    break;
                case CONSTRUCTOR:
                case METHOD:
                    pushScope( "::" + element );
                    processMethod( (ExecutableElement) element );
                    break;
                default:
                    throw new AssertionError( "???: " + kind );
                }
                popScope();
            }
        }
    }

    /**
     * Process variables. Fields, enum constants.
     */
    private void processField( VariableElement variableElement )
    {
        StringBuilder sb = new StringBuilder();
        addFieldName( sb, variableElement );
        addReturn( sb, variableElement.asType() );
        addModifiers( sb, variableElement );
        addConstantValue( sb, variableElement );
        publicElements.add( sb.toString() );
    }

    /**
     * Process executables. Constructors, methods.
     */
    private void processMethod( ExecutableElement element )
    {
        if ( element.getAnnotation( Deprecated.class ) != null )
        {
            inDeprecatedScope = true;
        }

        StringBuilder sb = new StringBuilder();
        addMethodName( sb, element );
        addParameters( sb, element );
        addReturn( sb, element.getReturnType() );
        addModifiers( sb, element );
        addExceptions( sb, element );

        publicElements.add( sb.toString() );

        inDeprecatedScope = false;
    }

    /**
     * Add implemented interfaces, e.g. {@code " implements Serializable, Comparable"}, or nothing if no interfaces are present.
     */
    private void addInterfaces( StringBuilder sb, TypeElement typeElement )
    {
        List<? extends TypeMirror> interfaces = typeElement.getInterfaces();
        if ( !interfaces.isEmpty() )
        {
            sb.append( interfaces.stream().map( this::encodeType ).collect( joining( ", ", " implements ", "" ) ) );
        }
    }

    /**
     * Add extended declaration, e.g. {@code " extends MyParent}. Note that even if {@code extends} is a keyword for interfaces, interfaces does not actually
     * have a super class.
     */
    private void addSuperClass( StringBuilder sb, TypeElement typeElement )
    {
        if ( typeElement.getKind() != ElementKind.INTERFACE && typeElement.getKind() != ElementKind.ANNOTATION_TYPE )
        {
            sb.append( " extends " );
            sb.append( encodeType( typeElement.getSuperclass() ) );
        }
    }

    private void addKindIdentifier( StringBuilder sb, TypeElement typeElement )
    {
        ElementKind kind = typeElement.getKind();
        switch ( kind )
        {
        case CLASS:
            sb.append( " class" );
            break;
        case INTERFACE:
            sb.append( " interface" );
            break;
        case ENUM:
            sb.append( " enum" );
            break;
        case ANNOTATION_TYPE:
            sb.append( " annotation" );
            break;
        default:
            error( "Unhandled ElementKind: " + kind );
        }
    }

    private void addTypeName( StringBuilder sb, TypeElement typeElement )
    {
        sb.append( typeElement.getQualifiedName() );
        addTypeParameter( sb, typeElement.getTypeParameters() );
    }

    /**
     * Takes a list of parameters and append it to the string builder, e.g. {@code "<K,V extends Object>"}
     */
    private void addTypeParameter( StringBuilder sb, List<? extends TypeParameterElement> typeParameters )
    {
        if ( !typeParameters.isEmpty() )
        {
            sb.append( typeParameters.stream().map( this::getGetBounds ).collect( joining( ", ", "<", ">" ) ) );
        }
    }

    private String getGetBounds( TypeParameterElement typeParameter )
    {
        List<String> bounds = typeParameter.getBounds().stream()
                .map( this::encodeType )
                .collect( toList() );
        if ( bounds.isEmpty() )
        {
            return typeParameter.toString();
        }
        return typeParameter + " extends " + String.join( " & ", bounds );
    }

    private void addFieldName( StringBuilder sb, VariableElement variableElement )
    {
        sb.append( encodeType( variableElement.getEnclosingElement().asType() ) );
        sb.append( "::" );
        sb.append( variableElement.getSimpleName() );
    }

    private void addParameters( StringBuilder sb, ExecutableElement element )
    {
        sb.append( '(' );
        List<? extends VariableElement> parameters = element.getParameters();
        for ( int i = 0; i < parameters.size(); i++ )
        {
            VariableElement parameter = parameters.get( i );
            sb.append( encodeType( parameter.asType() ) );
            if ( i != parameters.size() - 1 )
            {
                sb.append( ", " );
            }
            else // last
            {
                if ( element.isVarArgs() )
                {
                    if ( parameter.asType().getKind() == TypeKind.ARRAY )
                    {
                        sb.setLength( sb.length() - 2 ); // Strip "[]"
                    }
                    sb.append( "..." );
                }
            }
        }
        sb.append( ')' );
    }

    private void addReturn( StringBuilder sb, TypeMirror type )
    {
        sb.append( ' ' );
        sb.append( encodeType( type ) );
    }

    private void addMethodName( StringBuilder sb, ExecutableElement element )
    {
        sb.append( encodeType( element.getEnclosingElement().asType() ) );
        sb.append( "::" );
        addTypeParameter( sb, element.getTypeParameters() );

        if ( element.getKind() == ElementKind.CONSTRUCTOR )
        {
            sb.append( element.getEnclosingElement().getSimpleName() );
        }
        else
        {
            sb.append( element.getSimpleName() );
        }
    }

    private static void addModifiers( StringBuilder sb, Element element )
    {
        for ( Modifier modifier : element.getModifiers() )
        {
            sb.append( ' ' );
            sb.append( modifier );
        }
    }

    private void addExceptions( StringBuilder sb, ExecutableElement element )
    {
        List<? extends TypeMirror> exceptions = element.getThrownTypes();
        if ( !exceptions.isEmpty() )
        {
            sb.append( exceptions.stream().map( this::encodeType ).collect( joining( ", ", " throws ", "" ) ) );
        }
    }

    private static void addConstantValue( StringBuilder sb, VariableElement variableElement )
    {
        Object constantValue = variableElement.getConstantValue();
        if ( constantValue != null )
        {
            sb.append( " = " );
            sb.append( constantValue );
        }
    }

    private String encodeType( TypeMirror type )
    {
        TypeKind kind = type.getKind();
        if ( kind.isPrimitive() )
        {
            return kind.toString().toLowerCase( Locale.ROOT );
        }
        if ( kind == TypeKind.ARRAY )
        {
            ArrayType arrayType = (ArrayType) type;
            return encodeType( arrayType.getComponentType() ) + "[]";
        }
        if ( kind == TypeKind.TYPEVAR )
        {
            TypeVariable typeVariable = (TypeVariable) type;
            return "#" + typeVariable;
        }
        if ( kind == TypeKind.DECLARED )
        {
            DeclaredType referenceType = (DeclaredType) type;
            validatePublicVisibility( referenceType );
            return referenceType.toString();
        }
        if ( kind == TypeKind.VOID )
        {
            return "void";
        }

        error( "Unhandled type: " + kind );
        return "ERROR";
    }

    /**
     * Verify that the classes in the API is visible and annotated with {@link PublicApi}. The exception to this is when a method is marked as deprecated or
     * part of the java library.
     */
    private void validatePublicVisibility( DeclaredType declaredType )
    {
        if ( !validatedDeclaredTypes.add( declaredType.toString() ) )
        {
            return; // already validated
        }

        TypeElement element = (TypeElement) typeUtils.asElement( declaredType );
        if ( !element.getModifiers().contains( Modifier.PUBLIC ) )
        {
            error( "Element that is exposed through the API is not visible", element );
        }

        // Traverse type arguments, including bounds
        for ( TypeMirror typeArgument : declaredType.getTypeArguments() )
        {
            if ( typeArgument.getKind() == TypeKind.WILDCARD )
            {
                validateWildcard( (WildcardType) typeArgument );
            }
            if ( typeArgument.getKind() == TypeKind.DECLARED )
            {
                validatePublicVisibility( (DeclaredType) typeArgument );
            }
        }

        // We only care about our own classes
        if ( !declaredType.toString().startsWith( "org.neo4j." ) &&
                !declaredType.toString().startsWith( "com.neo4j." ) )
        {
            return;
        }

        if ( element.getNestingKind().isNested() )
        {
            TypeElement parent;
            do
            {
                parent = (TypeElement) element.getEnclosingElement();
            }
            while ( parent.getNestingKind().isNested() );

            assertAnnotated( element, parent, element.getQualifiedName() + "'s parent, " + parent.getQualifiedName() + "," );
        }
        else
        {
            // Top-level type must be annotated directly
            assertAnnotated( element, element, element.getQualifiedName() + " exposed through the API" );
        }
    }

    private void validateWildcard( WildcardType wildcardType )
    {
        filterWildcard( wildcardType.getExtendsBound() );
        filterWildcard( wildcardType.getSuperBound() );
    }

    private void filterWildcard( TypeMirror extendsBound )
    {
        if ( extendsBound != null )
        {
            TypeKind kind = extendsBound.getKind();
            if ( kind == TypeKind.DECLARED )
            {
                validatePublicVisibility( (DeclaredType) extendsBound );
            }
            if ( kind == TypeKind.WILDCARD )
            {
                validateWildcard( (WildcardType) extendsBound );
            }
        }
    }

    private void assertAnnotated( TypeElement element, TypeElement parent, String msg )
    {
        if ( parent.getAnnotation( IgnoreApiCheck.class ) != null )
        {
            return; // Stop traversing here
        }

        if ( parent.getAnnotation( PublicApi.class ) == null )
        {
            if ( inDeprecatedScope )
            {
                processingEnv.getMessager().printMessage( WARNING,
                        "Non-public element, " + element + ", is exposed through the API via a deprecated method", element );
            }
            else
            {
                error( msg + " is not marked with @" + PublicApi.class.getSimpleName(), element );
            }
        }
    }

    private void pushScope( String e )
    {
        scope.add( e );
    }

    private void popScope()
    {
        scope.remove( scope.size() - 1 );
    }

    private void info( String msg )
    {
        processingEnv.getMessager().printMessage( NOTE, msg );
    }

    private void error( String msg )
    {
        processingEnv.getMessager().printMessage( ERROR, msg );
    }

    private void error( String msg, Element element )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "Error processing " );
        scope.forEach( sb::append );
        sb.append( ':' );
        sb.append( System.lineSeparator() );
        sb.append( msg );
        processingEnv.getMessager().printMessage( ERROR, sb.toString(), element );
    }
}
