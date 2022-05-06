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
package io.github.linkedfactory.kvin;

import net.enilink.komma.core.URI;

/**
 * A linked list of (property, value) pairs where properties are {@link URI}s
 * and values are arbitrary objects. Duplicate properties are explicitly
 * permitted.
 * 
 * This is inspired by RDF and the scala.xml.MetaData implementation.
 */
public class Event extends Data<Event> {
	public static final Event NULL = new Event(null, null);

	public static final URI PROPERTY_VALUE = Data.PROPERTY_VALUE;

	public Event(URI property, Object value) {
		this(property, value, null);
	}

	public Event(URI property, Object value, Event next) {
		super(property, value, next == NULL ? null : next);
	}

	@Override
	public Event copy(Event next) {
		if (property == null) {
			return next;
		} else {
			return new Event(property, value, next);
		}
	}

	@Override
	protected Event NULL() {
		return NULL;
	}
}