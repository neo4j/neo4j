package org.neo4j.kernel.impl.messages;

/**
 * This is a beach head - if you are implementing code that prints human language
 * messages in any way, through errors, warnings, explanations or any other
 * mechanism - please help expand, refactor, move and use this this class to do
 * so.
 *
 * The intention of this is to slowly introduce a proper i18n message API,
 * backed by the standard i18n support in Java.
 */
public interface Messages
{
    Message proc_invalid_return_type_description =
            msg("Procedures must return a Stream of records, where a record is a concrete class " +
                "that you define, with public non-final fields defining the fields in the record. " +
                "If you'd like your procedure to return `%s`, you could define a record class like:\n" +
                "public class Output {\n" +
                "    public %s out;\n" +
                "}\n" +
                "\n" +
                "And then define your procedure as returning `Stream<Output>`.");

    Message proc_static_field_annotated_as_context =
            msg("The field `%s` in the class named `%s` is annotated as a @Context field," +
                "but it is static. @Context fields must be public, non-final and non-static," +
                "because they are reset each time a procedure is invoked.");

    interface Message
    {
        String defaultMessage();
    }

    static String get( Message msg, Object ... args )
    {
        return String.format( msg.defaultMessage(), args );
    }

    static Message msg(String defaultMessage)
    {
        return () -> defaultMessage;
    }
}
