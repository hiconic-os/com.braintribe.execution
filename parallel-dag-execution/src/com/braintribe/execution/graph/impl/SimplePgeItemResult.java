// ============================================================================
// Copyright BRAINTRIBE TECHNOLOGY GMBH, Austria, 2002-2022
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ============================================================================
package com.braintribe.execution.graph.impl;

import com.braintribe.execution.graph.api.ParallelGraphExecution.PgeItemResult;
import com.braintribe.execution.graph.api.ParallelGraphExecution.PgeItemStatus;

/**
 * @author peter.gazdik
 */
public class SimplePgeItemResult<N, R> implements PgeItemResult<N, R> {

	private final N item;
	private final R result;
	private final Throwable error;
	private final PgeItemStatus status;

	public SimplePgeItemResult(N item, R result, Throwable error, PgeItemStatus status) {
		this.item = item;
		this.result = result;
		this.error = error;
		this.status = status;
	}

	@Override
	public N getItem() {
		return item;
	}

	@Override
	public R getResult() {
		return result;
	}

	@Override
	public Throwable getError() {
		return error;
	}

	@Override
	public PgeItemStatus status() {
		return status;
	}

}
