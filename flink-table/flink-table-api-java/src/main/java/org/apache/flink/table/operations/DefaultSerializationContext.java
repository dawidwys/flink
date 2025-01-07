package org.apache.flink.table.operations;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.FunctionDefinition;

public class DefaultSerializationContext implements SerializationContext {
    @Override
    public String serializeInlineFunction(FunctionDefinition functionDefinition) {
        throw new TableException(
                "Only functions that have been registered before are serializable.");
    }
}
