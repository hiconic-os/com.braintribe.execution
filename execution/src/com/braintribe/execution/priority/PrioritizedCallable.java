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
package com.braintribe.execution.priority;

import java.util.concurrent.Callable;

public class PrioritizedCallable <T> extends Prioritized implements Callable<T>, Comparable<PrioritizedCallable<T>> {

	private Callable<T> delegate;
	
	public PrioritizedCallable(Callable<T> delegate, double priority, int insertionCount) {
		super(priority, insertionCount);
		this.delegate = delegate;
	}

	@Override
	public T call() throws Exception {
		return delegate.call();
	}
	
	@Override
	public int compareTo(PrioritizedCallable<T> o) {
		return super.compareTo(o);
	}

	@Override
	public String toString() {
		return "Callable with priority: "+priority;
	}
	
}
