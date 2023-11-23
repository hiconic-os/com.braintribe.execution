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

public class PriorityFuture<T> implements RunnableFuture<T>, Comparable<RunnableFuture<T>>, HasPriority {

    private RunnableFuture<T> delegate;
    private int insertionIndex;
    private double priority;

    public PriorityFuture(RunnableFuture<T> other, double priority, int insertionIndex) {
        this.delegate = other;
        this.priority = priority;
        this.insertionIndex = insertionIndex;
    }

    @Override
    public double getPriority() {
        return priority;
    }
    
    @Override
    public int getInsertionIndex() {
    	return insertionIndex;
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
	public int compareTo(RunnableFuture<T> or) {
		PriorityFuture<T> o = (PriorityFuture<T>) or;
		if (o.priority > this.priority) {
			return 1;
		} else if (o.priority < this.priority) {
			return -1;
		}
		if (o.insertionIndex < this.insertionIndex) {
			return 1;
		} else {
			return -1;
		}
	}
	
}