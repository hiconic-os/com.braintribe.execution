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
package com.braintribe.execution;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.braintribe.cfg.DestructionAware;
import com.braintribe.logging.Logger;

/**
 * <p>
 * A simple {@link ExecutorService} holder with standardized shutdown control.
 * 
 */
public class ExecutorServiceSupplier<T extends ExecutorService> implements Supplier<T>, DestructionAware {

	private static final Logger log = Logger.getLogger(ExecutorServiceSupplier.class);

	private T executor;
	private String name;
	private Runnable shutdown;
	private TimeUnit awaitTerminationUnit = TimeUnit.SECONDS;
	private long awaitTermination = 0;

	public static <T extends ExecutorService> ExecutorServiceSupplier<T> create() {
		return new ExecutorServiceSupplier<T>();
	}

	public ExecutorServiceSupplier<T> id(String executorName) {
		this.name = executorName;
		return this;
	}

	public ExecutorServiceSupplier<T> executor(T executorService) {
		this.executor = executorService;
		return this;
	}

	public ExecutorServiceSupplier<T> shutdown(Runnable function) {
		this.shutdown = function;
		return this;
	}

	public ExecutorServiceSupplier<T> awaitTermination(long value) {
		this.awaitTermination = value;
		return this;
	}

	public ExecutorServiceSupplier<T> awaitTermination(TimeUnit unit, long value) {
		this.awaitTerminationUnit = unit;
		this.awaitTermination = value;
		return this;
	}

	@Override
	public T get() {
		return executor;
	}

	@Override
	public void preDestroy() {

		if (executor == null) {
			return;
		}

		log.info("Shutting down " + id());

		if (shutdown == null) {
			executor.shutdownNow();
		} else {
			shutdown.run();
		}

		awaitTermination();

	}

	protected void awaitTermination() {
		if (awaitTermination > 0) {
			try {
				if (!executor.awaitTermination(awaitTermination, awaitTerminationUnit)) {
					log.warn("Timed out while waiting for " + id() + " to terminate");
				}
			} catch (InterruptedException ex) {
				log.warn("Interrupted while waiting for " + id() + " to terminate");
				Thread.currentThread().interrupt();
			}
		}
	}

	protected String id() {
		return (name != null) ? name : (executor != null) ? executor.toString() : "null";
	}

	
}
