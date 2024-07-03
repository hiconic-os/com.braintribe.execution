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
package com.braintribe.execution.generic;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ContextualizedFuture <T,U> implements Future<T> {

	protected Future<T> delegate;
	protected U context;
	protected Instant created;
	private Instant started;

	public ContextualizedFuture(Future<T> delegate, U context) {
		this.delegate = delegate;
		this.context = context;
		this.created = Clock.systemUTC().instant();
	}
	
	/**
	 * Only to be called by {@link ServiceExecution} as it will provide the delegate at a later step.
	 * @param context The context of the future.
	 */
	protected ContextualizedFuture(U context) {
		this.context = context;
		this.created = Clock.systemUTC().instant();
	}
	protected void setDelegate(Future<T> delegate) {
		this.delegate = delegate;
	}
	protected void setStartInstant(Instant startInstant) {
		this.started = startInstant;
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
	
	/**
	 * Almost the same as {@link #get(long, TimeUnit)}, but it subtracts the time between creation of the Future and the
	 * invocation of {@link #getWithRemainingTime(long, TimeUnit)} so that consecutive waits are not culmulated.
	 *  
	 * @param timeout The total wait the caller is willing to give the job
	 * @param unit The time unit for the timeout
	 * @return The actual result.
     * @throws CancellationException if the computation was cancelled
     * @throws ExecutionException if the computation threw an
     * exception
     * @throws InterruptedException if the current thread was interrupted
     * while waiting
     * @throws TimeoutException if the wait timed out
	 */
	public T getWithRemainingTime(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		long maxWaitMs = unit.toMillis(timeout);
		long ageMs = getAge().toMillis();
		long remainingWaitMs = maxWaitMs - ageMs;
		if (remainingWaitMs < 0) {
			remainingWaitMs = 0;
		}
		return delegate.get(remainingWaitMs, TimeUnit.MILLISECONDS);
	}

	/**
	 * Almost the same as {@link #get(long, TimeUnit)}, but it subtracts the time between start of the Future and the
	 * invocation of {@link #getWithRemainingTime(long, TimeUnit)} so that consecutive waits are not culmulated.
	 * Only call this method when {@link ServiceExecution#submit(java.util.concurrent.Callable, Object, java.util.concurrent.ExecutorService)}
	 * has been used to create the Future. Otherwise, it cannot be guaranteed that the starting instant will be known.
	 *  
	 * @param timeout The total wait the caller is willing to give the job
	 * @param unit The time unit for the timeout
	 * @param waitForStartTimeout The maximum amount of time it should wait for the future to actually start.
	 * @return The actual result.
     * @throws CancellationException if the computation was cancelled
     * @throws ExecutionException if the computation threw an
     * exception
     * @throws InterruptedException if the current thread was interrupted
     * while waiting
     * @throws TimeoutException if the wait timed out
	 */
	public T getWithRemainingTime(long timeout, TimeUnit unit, Duration waitForStartTimeout) throws InterruptedException, ExecutionException, TimeoutException {
		long maxWaitMs = unit.toMillis(timeout);
		
		long waitForStartUntil = System.currentTimeMillis() + waitForStartTimeout.toMillis();
		Duration runningDuration = getRunningDuration();
		while (runningDuration == null) {
			Thread.sleep(1000L);
			runningDuration = getRunningDuration();
			if (System.currentTimeMillis() > waitForStartUntil) {
				break;
			}
		}
		long ageMs = 0L;
		if (started != null) {
			ageMs = getRunningDuration().toMillis();	
		} else {
			ageMs = getAge().toMillis();
		}
		long remainingWaitMs = maxWaitMs - ageMs;
		if (remainingWaitMs < 0) {
			remainingWaitMs = 0;
		}
		return delegate.get(remainingWaitMs, TimeUnit.MILLISECONDS);
	}

	public U getContext() {
		return this.context;
	}
	
	public Duration getAge() {
		return Duration.between(created, Clock.systemUTC().instant());
	}
	
	public Duration getRunningDuration() {
		if (started == null) {
			return null;
		}
		return Duration.between(started, Clock.systemUTC().instant());
	}
}
