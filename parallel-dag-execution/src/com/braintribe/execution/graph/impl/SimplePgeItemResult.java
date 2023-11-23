// ============================================================================
// Copyright BRAINTRIBE TECHNOLOGY GMBH, Austria, 2002-2022
// 
// This library is free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
// 
// This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License along with this library; See http://www.gnu.org/licenses/.
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
