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
package com.braintribe.execution.queue;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Special implementation of the LinkedBlockinguQueue
 * 
 * Its intended use is for ThreadPoolExecutors only where corePoolSize == maxPoolSize.
 * This can be used to block a submit to a thread pool until one of the working threads is available again.
 */
public class LimitedQueue<T> extends LinkedBlockingQueue<T> {
	
	private static final long serialVersionUID = 1L;

	public LimitedQueue(int maxSize) {
		super(maxSize);
	}

	@Override
	public boolean offer(T e) {
		// turn offer() and add() into a blocking calls (unless interrupted)
		try {
			put(e);
			return true;
		} catch(InterruptedException ie) {
			Thread.currentThread().interrupt();
		}
		return false;
	}

}
