package org.apache.flink.table.expressions;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.FunctionDefinition;

public class DefaultExpressionSerializationContext implements ExpressionSerializationContext {
    @Override
    public String serializeInlineFunction(FunctionDefinition functionDefinition) {
        throw new TableException(
                "Only functions that have been registered before are serializable.");
    }
}
