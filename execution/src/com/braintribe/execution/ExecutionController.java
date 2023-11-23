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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import com.braintribe.cfg.DestructionAware;
import com.braintribe.cfg.Required;

public class ExecutionController implements DestructionAware {

	protected Executor executor = null;
	protected List<Callable<?>> callables = null;

	public void initialize() {

		if (this.executor instanceof ExecutorService) {

			final ExecutorService es = (ExecutorService) this.executor;

			for (final Callable<?> callable : this.callables) {
				es.submit(callable);
			}
		}
	}

	public Executor getExecutor() {
		return this.executor;
	}

	@Required
	public void setExecutor(final Executor executor) {
		this.executor = executor;
	}

	public List<Callable<?>> getCallables() {
		return this.callables;
	}

	@Required
	public void setCallables(final List<Callable<?>> callables) {
		this.callables = callables;
	}

	@Override
	public void preDestroy() {
		if (this.executor != null) {
			if (this.executor instanceof ExecutorService) {
				final ExecutorService es = (ExecutorService) this.executor;
				es.shutdownNow();
			}
		}

	}
}
