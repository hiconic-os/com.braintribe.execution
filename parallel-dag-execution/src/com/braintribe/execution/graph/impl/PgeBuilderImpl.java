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
