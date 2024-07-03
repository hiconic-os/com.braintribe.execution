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
package com.braintribe.execution.virtual;

import java.time.Instant;
import java.util.concurrent.Semaphore;

import com.braintribe.exception.Exceptions;
import com.braintribe.utils.date.NanoClock;

public class VirtualRunnable implements Runnable {

	private Runnable delegate;
	private Instant creationInstant;
	private VirtualThreadExecutor executor;
	private Semaphore semaphore;

	public VirtualRunnable(Runnable delegate, VirtualThreadExecutor executor, Semaphore semaphore) {
		this.executor = executor;
		this.creationInstant = NanoClock.INSTANCE.instant();
		this.delegate = delegate;
		this.semaphore = semaphore;
	}

	@Override
	public void run() {
		try {
			semaphore.acquire();
		} catch (InterruptedException e) {
			throw Exceptions.unchecked(e);
		}
		String execId = executor.beforeExecute();
		Instant executionInstant = NanoClock.INSTANCE.instant();
		try {
			this.delegate.run();
		} finally {
			Instant finishedInstant = NanoClock.INSTANCE.instant();

			semaphore.release();
			executor.afterExecute(execId);
			executor.executionFinished(creationInstant, executionInstant, finishedInstant);
		}
	}
}
