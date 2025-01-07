package org.apache.flink.table.operations;

import org.apache.flink.table.expressions.SerializationContext;
import org.apache.flink.table.functions.FunctionDefinition;

public class ExpressionSerializationContextAdapter implements SerializationContext {

    private final org.apache.flink.table.operations.SerializationContext context;

    public ExpressionSerializationContextAdapter(org.apache.flink.table.operations.SerializationContext context) {
        this.context = context;
    }

    @Override
    public String serializeInlineFunction(FunctionDefinition functionDefinition) {
        return context.serializeInlineFunction(functionDefinition);
    }
}
