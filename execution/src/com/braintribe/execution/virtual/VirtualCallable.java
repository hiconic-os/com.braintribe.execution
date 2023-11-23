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
package com.braintribe.execution.virtual;

import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import com.braintribe.exception.Exceptions;
import com.braintribe.utils.date.NanoClock;

public class VirtualCallable<T> implements Callable<T> {

	private Callable<T> delegate;
	private Instant creationInstant;
	private VirtualThreadExecutor executor;
	private Semaphore semaphore;

	public VirtualCallable(Callable<T> delegate, VirtualThreadExecutor executor, Semaphore semaphore) {
		this.executor = executor;
		this.creationInstant = NanoClock.INSTANCE.instant();
		this.delegate = delegate;
		this.semaphore = semaphore;
	}

	@Override
	public T call() throws Exception {
		try {
			semaphore.acquire();
		} catch (InterruptedException e) {
			throw Exceptions.unchecked(e);
		}
		String execId = executor.beforeExecute();
		Instant executionInstant = NanoClock.INSTANCE.instant();
		try {
			return this.delegate.call();
		} finally {
			Instant finishedInstant = NanoClock.INSTANCE.instant();

			semaphore.release();
			executor.afterExecute(execId);
			executor.executionFinished(creationInstant, executionInstant, finishedInstant);
		}
	}
}
