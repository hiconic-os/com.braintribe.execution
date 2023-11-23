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
package com.braintribe.execution.context;

import java.util.concurrent.Callable;

import com.braintribe.common.attribute.AttributeContext;
import com.braintribe.utils.collection.impl.AttributeContexts;

public class AttributeContextTransferCallable<T> implements Callable<T> {
	private AttributeContext attributeContext = AttributeContexts.peek();
	private Callable<T> callable;

	public AttributeContextTransferCallable(Callable<T> callable) {
		super();
		this.callable = callable;
	}

	@Override
	public T call() throws Exception {
		if (attributeContext != AttributeContexts.empty()) {
			return AttributeContexts.with(attributeContext).call(callable);
		} else {
			return callable.call();
		}
	}
}