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
package com.braintribe.execution.generic;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

public class ContextualizedFutureTest {

	@Test
	public void testContext() throws Exception {
		ExecutorService pool = Executors.newFixedThreadPool(1);

		try {
			Future<String> future = pool.submit(() -> {
				System.out.println("Do something");
				return "context1";
			});

			ContextualizedFuture<String,String> cf = new ContextualizedFuture<>(future, "context2");
			
			String result = cf.get();
			
			assertThat(result).isEqualTo("context1");
			
		} finally {
			pool.shutdown();
		}
		
	}
	
}
