/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;

/**
 * Class that implements visitor pattern. It allows type safe logic on top of tree
 * of {@link TableOperation}s.
 */
@Internal
public interface TableOperationVisitor<T> {
	T visitProject(ProjectTableOperation projection);

	T visitAggregate(AggregationTableOperation aggregation);

	T visitAlgebraicOperation(AlgebraicTableOperation algebraicOperation);

	T visitFilter(FilterTableOperation filter);

	T visitDistinct(DistinctTableOperation distinct);

	T visitSort(SortTableOperation sort);

	T visitOther(TableOperation other);
}
