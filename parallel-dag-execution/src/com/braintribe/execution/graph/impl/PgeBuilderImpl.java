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
package com.braintribe.execution.graph.impl;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

import com.braintribe.common.lcd.function.CheckedFunction;
import com.braintribe.exception.Exceptions;
import com.braintribe.execution.graph.api.ParallelGraphExecution.PgeBuilder;
import com.braintribe.execution.graph.api.ParallelGraphExecution.PgeExecutor;
import com.braintribe.execution.graph.api.ParallelGraphExecution.PgeResult;
import com.braintribe.execution.virtual.VirtualThreadExecutorBuilder;

/**
 * @author peter.gazdik
 */
public class PgeBuilderImpl<N> implements PgeBuilder<N> {

	private final String name;
	private final Iterable<? extends N> nodes;

	private Supplier<ExecutorService> threadPoolExecutorSupplier;
	private boolean shutDownThreadPool;
	private Function<N, Iterable<? extends N>> dependencyResolver;
	private Function<N, Iterable<? extends N>> dependerResolver;

	public PgeBuilderImpl(String name, Iterable<? extends N> nodes) {
		this.name = name;
		this.nodes = nodes;
	}

	// ###############################################
	// ## . . . . . . . Graph-edges . . . . . . . . ##
	// ###############################################

	@Override
	public PgeBuilder<N> itemsToProcessFirst(Function<N, Iterable<? extends N>> dependencyResolver) {
		if (dependerResolver != null)
			throwIllegalState("Cannot configure 'itemsToProcessFirst' as 'itemsToProcessAfter' was already specified");

		this.dependencyResolver = dependencyResolver;
		return this;
	}

	@Override
	public PgeBuilder<N> itemsToProcessAfter(Function<N, Iterable<? extends N>> dependerResolver) {
		if (dependencyResolver != null)
			throwIllegalState("Cannot configure 'itemsToProcessAfter' as 'itemsToProcessFirst' was already specified.");

		this.dependerResolver = dependerResolver;
		return this;
	}

	// ###############################################
	// ## . . . . . . . Thread pool . . . . . . . . ##
	// ###############################################

	@Override
	public PgeBuilder<N> withThreadPool(int nThreads) {
		return withThreadPoolExecutorFactory(() -> newThreadPoolExecutor(nThreads));
	}

	private ExecutorService newThreadPoolExecutor(int nThreads) {
		return VirtualThreadExecutorBuilder.newPool().concurrency(nThreads).threadNamePrefix(name).description("ParallelGraphExecutor").build();
	}

	@Override
	public PgeBuilder<N> withThreadPoolExecutorSupplier(Supplier<ExecutorService> threadPoolExecutorSupplier) {
		this.threadPoolExecutorSupplier = threadPoolExecutorSupplier;
		this.shutDownThreadPool = false;
		return this;
	}

	@Override
	public PgeBuilder<N> withThreadPoolExecutorFactory(Supplier<ExecutorService> threadPoolExecutorFactory) {
		this.threadPoolExecutorSupplier = threadPoolExecutorFactory;
		this.shutDownThreadPool = true;
		return this;
	}

	// ###############################################
	// ## . . . . . . . . Terminals . . . . . . . . ##
	// ###############################################

	@Override
	public <R> PgeResult<N, R> compute(CheckedFunction<? super N, ? extends R, Exception> processor) {
		return prepareRunner().compute(processor);
	}

	// ###############################################
	// ## . . . . . . . Build runner . . . . . . . .##
	// ###############################################

	@Override
	public PgeExecutor<N> prepareRunner() {
		try {
			PgeGraph<N> graph = buildGraph();
			return new PgeExecutorImpl<N>(name, threadPoolExecutorSupplier, shutDownThreadPool, graph);

		} catch (RuntimeException e) {
			throw Exceptions.contextualize(e, errorMessage(""));
		}
	}

	private PgeGraph<N> buildGraph() {
		if (dependencyResolver != null)
			return PgeGraph.forChildResolver(nodes, dependencyResolver);

		else if (dependerResolver != null)
			return PgeGraph.forParentResolver(nodes, dependerResolver);

		else
			return throwIllegalState("Neither  'itemsToProcessAfter', nor 'itemsToProcessFirst' was specified.");
	}

	private <T> T throwIllegalState(String message) {
		throw new IllegalStateException(errorMessage(message));
	}

	private String errorMessage(String detail) {
		return "Error in parallel execution '" + name + "'. " + detail;
	}

}
