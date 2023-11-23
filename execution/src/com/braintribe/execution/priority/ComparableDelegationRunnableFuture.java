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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ComparableDelegationRunnableFuture<T> implements RunnableFuture<T>, Comparable<ComparableDelegationRunnableFuture<T>> {

    private RunnableFuture<T> delegate;
    private Comparable<Object> comparableDelegate;

    public ComparableDelegationRunnableFuture(RunnableFuture<T> other, Comparable<Object> compareDelegate) {
        this.delegate = other;
		this.comparableDelegate = compareDelegate;
    }

    @Override
	public boolean cancel(boolean mayInterruptIfRunning) {
        return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
	public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
	public boolean isDone() {
        return delegate.isDone();
    }

    @Override
	public T get() throws InterruptedException, ExecutionException {
        return delegate.get();
    }

    @Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.get(timeout, unit);
    }

    @Override
	public void run() {
        delegate.run();
    }

	@Override
	public int compareTo(ComparableDelegationRunnableFuture<T> o) {
		return comparableDelegate.compareTo(o.comparableDelegate);
	}
	
}