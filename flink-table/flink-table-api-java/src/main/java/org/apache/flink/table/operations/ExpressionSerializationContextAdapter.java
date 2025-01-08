package org.apache.flink.table.operations;

import org.apache.flink.table.expressions.ExpressionSerializationContext;
import org.apache.flink.table.functions.FunctionDefinition;

public class ExpressionSerializationContextAdapter implements ExpressionSerializationContext {

    private final OperationSerializationContext context;

    public ExpressionSerializationContextAdapter(OperationSerializationContext context) {
        this.context = context;
    }

    @Override
    public String serializeInlineFunction(FunctionDefinition functionDefinition) {
        return context.serializeInlineFunction(functionDefinition);
    }
}
