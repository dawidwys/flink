package org.apache.flink.table.operations;

import org.apache.flink.table.functions.FunctionDefinition;

public interface OperationSerializationContext {
    String serializeInlineFunction(FunctionDefinition functionDefinition);
}
