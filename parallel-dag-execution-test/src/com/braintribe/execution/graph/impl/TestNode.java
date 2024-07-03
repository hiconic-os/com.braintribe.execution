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

import static com.braintribe.utils.lcd.CollectionTools2.newList;

import java.util.List;

class TestNode {
	public final String name;

	public final List<TestNode> parents = newList();
	public final List<TestNode> children = newList();

	public Long timeOfExecution;
	
	public TestNode(String name) {
		this.name = name;
	}

	public List<TestNode> getParents() {
		return parents;
	}

	public List<TestNode> getChildren() {
		return children;
	}

	@Override
	public String toString() {
		return "[" + name + "]";
	}
}
