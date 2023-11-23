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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CountingVirtualThreadFactory implements ThreadFactory {

	private static final AtomicInteger poolNumber = new AtomicInteger(1);

	private final String namePrefix;
	private final AtomicInteger threadNumber = new AtomicInteger(1);

	public CountingVirtualThreadFactory(String prefix) {
		this.namePrefix = namePrefix(prefix);
	}

	private static String namePrefix(String prefix) {
		if (prefix == null || prefix.trim().length() == 0)
			prefix = "pool";
		return prefix + "-" + poolNumber.getAndIncrement() + "-thread-";
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread thread = Thread.ofVirtual().name(namePrefix + threadNumber.getAndIncrement()).unstarted(r);
		return thread;
	}
}
