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
package com.braintribe.execution.generic;

import java.time.Clock;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.braintribe.execution.priority.PrioritizedThreadPoolExecutor;
import com.braintribe.thread.api.ThreadContextScoping;

/**
 * Helper class that wraps a provided Callable object and submit it to the provided executor service. The wrapper makes
 * sure that the newly created {@link ContextualizedFuture} gets informed when the task is actually executed. This
 * allows to compute the maximum time the caller should wait for a result based on the actual processing time instead of
 * the time it spent in the queue.
 */
public class ServiceExecution {

	public static <T, U> ContextualizedFuture<T, U> submit(Callable<T> callable, U context, ExecutorService executor) {
		return submit(callable, context, executor, (ThreadContextScoping) null);
	}

	public static <T, U> ContextualizedFuture<T, U> submit(Callable<T> callable, U context, ExecutorService executor,
			ThreadContextScoping threadContextScoping) {
		ContextualizedFuture<T, U> contextualizedFuture = new ContextualizedFuture<>(context);
		CallableWrapper<T, U> wrapper = new CallableWrapper<>(callable, contextualizedFuture);

		Callable<T> callableForSubmit = null;
		if (threadContextScoping != null) {
			callableForSubmit = threadContextScoping.bindContext(wrapper);
		} else {
			callableForSubmit = wrapper;
		}
		Future<T> future = executor.submit(callableForSubmit);
		contextualizedFuture.setDelegate(future);
		return contextualizedFuture;
	}

	public static <T, U> ContextualizedFuture<T, U> submit(Callable<T> callable, U context, PrioritizedThreadPoolExecutor executor, Double priority) {
		return submit(callable, context, executor, priority, (ThreadContextScoping) null);
	}

	public static <T, U> ContextualizedFuture<T, U> submit(Callable<T> callable, U context, PrioritizedThreadPoolExecutor executor, Double priority,
			ThreadContextScoping threadContextScoping) {
		ContextualizedFuture<T, U> contextualizedFuture = new ContextualizedFuture<>(context);
		CallableWrapper<T, U> wrapper = new CallableWrapper<>(callable, contextualizedFuture);
		Callable<T> callableForSubmit = null;
		if (threadContextScoping != null) {
			callableForSubmit = threadContextScoping.bindContext(wrapper);
		} else {
			callableForSubmit = wrapper;
		}

		Future<T> future = executor.submit(callableForSubmit, priority);
		contextualizedFuture.setDelegate(future);
		return contextualizedFuture;
	}

	private static class CallableWrapper<T, U> implements Callable<T> {

		private Callable<T> delegate;
		private ContextualizedFuture<T, U> future;

		public CallableWrapper(Callable<T> delegate, ContextualizedFuture<T, U> future) {
			super();
			this.delegate = delegate;
			this.future = future;
		}

		@Override
		public T call() throws Exception {
			future.setStartInstant(Clock.systemUTC().instant());
			return delegate.call();
		}

	}
}
