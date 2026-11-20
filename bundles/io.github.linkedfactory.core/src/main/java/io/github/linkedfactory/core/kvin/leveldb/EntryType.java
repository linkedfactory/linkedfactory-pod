/*
 * Copyright (c) 2022 Fraunhofer IWU.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.linkedfactory.core.kvin.leveldb;

/**
 * Types of the URI-to-ID mappings in the ids database of {@link KvinLevelDb}.
 */
public enum EntryType {
	SubjectToId(0, 1),
	PropertyToId(1, 3),
	ResourceToId(2, 5),
	ContextToId(3, 7);

	private final int index;
	private final int id;

	EntryType(int index, int id) {
		this.index = index;
		this.id = id;
	}

	/**
	 * The position of this type in arrays that hold one value per type.
	 */
	public int index() {
		return index;
	}

	/**
	 * The key prefix of the forward mapping URI -> ID.
	 */
	public int id() {
		return id;
	}

	/**
	 * The key prefix of the reverse mapping ID -> URI.
	 */
	public int reverse() {
		return id + 1;
	}
}
