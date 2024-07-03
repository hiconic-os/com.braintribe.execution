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