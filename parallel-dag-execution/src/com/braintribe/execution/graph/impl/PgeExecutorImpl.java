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

import static com.braintribe.utils.lcd.CollectionTools2.newList;
import static com.braintribe.utils.lcd.CollectionTools2.newMap;
import static com.braintribe.utils.lcd.CollectionTools2.newSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.braintribe.common.lcd.function.CheckedFunction;
import com.braintribe.execution.graph.api.ParallelGraphExecution.PgeExecutor;
import com.braintribe.execution.graph.api.ParallelGraphExecution.PgeItemResult;
import com.braintribe.execution.graph.api.ParallelGraphExecution.PgeItemStatus;
import com.braintribe.execution.graph.api.ParallelGraphExecution.PgeResult;

/**
 * @author peter.gazdik
 */
public class PgeExecutorImpl<N> implements PgeExecutor<N> {

	private final String name;
	private final PgeGraph<N> graph;
	private final Supplier<ExecutorService> threadPoolExecutorSupplier;
	private final boolean shutDownThreadPool;

	public PgeExecutorImpl(String name, Supplier<ExecutorService> threadPoolExecutorSupplier, boolean shutDownThreadPool, PgeGraph<N> graph) {
		this.name = name;
		this.threadPoolExecutorSupplier = threadPoolExecutorSupplier;
		this.shutDownThreadPool = shutDownThreadPool;
		this.graph = graph;
	}

	@Override
	public <R> PgeResult<N, R> compute(CheckedFunction<? super N, ? extends R, Exception> processor) {
		return new PgeComputation<R>(processor, threadPoolExecutorSupplier.get()).compute();
	}

	private class ComputationNode<R> {
		final PgeNode<N> pgeNode;
		final List<ComputationNode<R>> dependers = newList();
		final Set<ComputationNode<R>> dependencies = newSet();

		R result;
		Throwable error;
		PgeItemStatus status = PgeItemStatus.created;

		public ComputationNode(PgeNode<N> pgeNode) {
			this.pgeNode = pgeNode;
		}

		/* package */ N item() {
			return pgeNode.item;
		}

		/* package */ PgeItemResult<N, R> toItemResult() {
			return new SimplePgeItemResult<N, R>(item(), result, error, status);
		}

		/**
		 * This method notifies this node that it's dependency has already been executed, and this method returns <tt>true</tt>
		 * iff this node can now be executed, i.e. iff there are no other dependencies this node has to wait for.
		 */
		synchronized boolean s_onDependencyResolved(ComputationNode<R> dependency) {
			dependencies.remove(dependency);
			return dependencies.isEmpty();
		}
	}

	/**
	 * Implementation details: This runs given processor on the nodes of enclosing {@link PgeGraph} respecting the execution
	 * dependencies (i.e. child-first) in a synchronous way.
	 * <p>
	 * In the first step a {@link CountDownLatch} is created and set to the number of nodes.
	 * <p>
	 * After each node is successfully processed, the CDL is decreased. Main thread is waiting on the CDL and when it hits
	 * zero it returns a result.
	 * <p>
	 * In case of an error, the s_hasError flag is switched to true, which makes sure that no new tasks will be
	 * {@link #s_submit submitted}, nor {@link #compute(ComputationNode) executed} in case they were already submitted, but
	 * not started yet. Along the way we also keep track of the number of submitted tasks and once it hits zero (i.e. all
	 * running tasks were finished and no new are being started), we interrupt the main thread, which can tell an error has
	 * happened because of the s_hasError flag.
	 */
	private class PgeComputation<R> {

		// deps have to be computed first
		private final CheckedFunction<? super N, ? extends R, Exception> processor;
		private final ExecutorService threadPoolExecutor;

		private final Map<PgeNode<N>, ComputationNode<R>> pgeToComputationNode = computeNodeToDeps();

		/**
		 * This is the latch the main thread will be waiting for to wait for the computation to finish. If everything is
		 * processed properly,
		 */
		private final CountDownLatch cdl = new CountDownLatch(pgeToComputationNode.size());
		private final Thread mainThraed = Thread.currentThread();
		private int s_submittedComputations = 0;
		private volatile boolean s_hasError; // when simply checking if there is an error to not run new tasks, we don't sync

		public PgeComputation(CheckedFunction<? super N, ? extends R, Exception> processor, ExecutorService threadPoolExecutor) {
			this.processor = processor;
			this.threadPoolExecutor = threadPoolExecutor;
		}

		private Map<PgeNode<N>, ComputationNode<R>> computeNodeToDeps() {
			Map<PgeNode<N>, ComputationNode<R>> result = newMap();

			fillNodeToDeps(graph.leafs, result, newSet());

			return result;
		}

		private void fillNodeToDeps(Collection<PgeNode<N>> nodes, Map<PgeNode<N>, ComputationNode<R>> result, Set<PgeNode<N>> visited) {
			for (PgeNode<N> node : nodes) {
				if (!visited.add(node))
					continue;

				ComputationNode<R> cNode = acquireComputationNode(node, result);
				for (PgeNode<N> parentNode : node.parents) {
					PgeExecutorImpl<N>.ComputationNode<R> parentCNode = acquireComputationNode(parentNode, result);
					cNode.dependers.add(parentCNode);
					parentCNode.dependencies.add(cNode);
				}

				fillNodeToDeps(node.parents, result, visited);
			}
		}

		private ComputationNode<R> acquireComputationNode(PgeNode<N> node, Map<PgeNode<N>, ComputationNode<R>> pgeToComputationNode) {
			// for whatever reason using method reference on a constructor here does not compile with Javac, at least not my old
			// java 8
			return pgeToComputationNode.computeIfAbsent(node, n -> new ComputationNode<>(n));
		}

		public PgeResult<N, R> compute() {
			for (PgeNode<N> pgeNode : graph.leafs)
				s_submit(pgeToComputationNode.get(pgeNode));

			waitForComputationToFinish();

			return buildComputationResult();
		}

		private synchronized void s_submit(ComputationNode<R> cNode) {
			// If there is already an error, we don't want to submit another task
			if (s_hasError)
				return;

			cNode.status = PgeItemStatus.submitted;

			s_submittedComputations++;
			threadPoolExecutor.submit(() -> compute(cNode));
		}

		private synchronized void onComputationEnd() {
			s_submittedComputations--;
			if (s_hasError && s_submittedComputations == 0)
				mainThraed.interrupt();
		}

		private R compute(ComputationNode<R> cNode) {
			try {
				// If there is already an error, we cancel the actual execution
				if (s_hasError) {
					cNode.status = PgeItemStatus.cancelled;
					return null;
				}

				R result = processor.apply(cNode.pgeNode.item);
				onComputationOk(cNode, result);
				return result;

			} catch (Throwable e) {
				s_onComputationError(cNode, e);
				return null;

			} finally {
				onComputationEnd();
			}
		}

		private synchronized void s_onComputationError(PgeExecutorImpl<N>.ComputationNode<R> cNode, Throwable e) {
			cNode.error = e;
			cNode.status = PgeItemStatus.failed;
			s_hasError = true;
		}

		private void onComputationOk(PgeExecutorImpl<N>.ComputationNode<R> cNode, R result) {
			cNode.result = result;
			cNode.status = PgeItemStatus.finished;
			cdl.countDown();

			for (ComputationNode<R> depender : cNode.dependers)
				if (depender.s_onDependencyResolved(cNode))
					s_submit(depender);
		}

		private void waitForComputationToFinish() {
			try {
				cdl.await();
			} catch (InterruptedException e) {
				handleInterrupted(e);
			}
		}

		private synchronized void handleInterrupted(InterruptedException e) {
			if (shutDownThreadPool)
				threadPoolExecutor.shutdownNow();

			if (s_hasError)
				return;
			else
				throw new RuntimeException("Something happened that interrupted parallel graph execution: " + name, e);
		}

		private PgeResult<N, R> buildComputationResult() {
			return new SimplePgeResult<N, R>(buildResultMap());
		}

		private Map<N, PgeItemResult<N, R>> buildResultMap() {
			return pgeToComputationNode.values().stream().collect(Collectors.toMap( //
					ComputationNode::item, //
					ComputationNode::toItemResult //
			));
		}

	}

}
