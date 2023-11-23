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

import java.util.Map;

import com.braintribe.execution.graph.api.ParallelGraphExecution.PgeItemResult;
import com.braintribe.execution.graph.api.ParallelGraphExecution.PgeResult;

/**
 * @author peter.gazdik
 */
public class SimplePgeResult<N, R> implements PgeResult<N, R> {

	private final Map<N, PgeItemResult<N, R>> itemsResults;
	private final boolean hasError;

	public SimplePgeResult(Map<N, PgeItemResult<N, R>> itemsResults) {
		this.itemsResults = itemsResults;
		this.hasError = hasError(itemsResults);
	}

	private boolean hasError(Map<N, PgeItemResult<N, R>> itemsResults) {
		return itemsResults.values().stream() //
				.filter(itemResult -> itemResult.getError() != null) //
				.findAny() //
				.isPresent();
	}

	@Override
	public boolean hasError() {
		return hasError;
	}

	@Override
	public Map<N, PgeItemResult<N, R>> itemResulsts() {
		return itemsResults;
	}

}
