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
