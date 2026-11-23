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
package io.github.linkedfactory.core.rdf4j.kvin;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;
import io.github.linkedfactory.core.rdf4j.common.BaseFederatedServiceResolver;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class KvinSailTest {
	final URI valueProperty = URIs.createURI("property:value");

	File storeDirectory;
	Kvin store;
	Repository repository;

	@Test
	public void basicTest() {
		RepositoryConnection conn = repository.getConnection();
		ValueFactory vf = repository.getValueFactory();

		try {
			conn.add(vf.createIRI(itemUri(1).toString()), vf.createIRI(valueProperty.toString()),
					vf.createLiteral(5), vf.createIRI("kvin:"));
		} finally {
			conn.close();
		}
	}

	URI itemUri(int nr) {
		return URIs.createURI("http://example.org/item-" + nr);
	}

	@Before
	public void init() throws IOException {
		createStore();
		createRepository();
	}

	void createStore() throws IOException {
		storeDirectory = Files.createTempDirectory("leveldb-test-").toFile();
		store = new KvinLevelDb(storeDirectory);
	}

	void createRepository() {
		MemoryStore memoryStore = new MemoryStore();
		KvinSail kvinSail = new KvinSail(store, memoryStore);
		SailRepository sailRepository = new SailRepository(kvinSail);

		sailRepository.setFederatedServiceResolver(new BaseFederatedServiceResolver() {
			@Override
			public KvinFederatedService createService(String url) {
				return new KvinFederatedService(store, this::getExecutorService, null, false);
			}
		});

		sailRepository.init();
		this.repository = sailRepository;
	}

	@After
	public void closeRepository() {
		repository.shutDown();
	}

	@After
	public void closeStore() throws IOException {
		store.close();
		store = null;
		deleteDirectory(storeDirectory.toPath());
	}

	void deleteDirectory(Path dir) throws IOException {
		// delete store directory
		Files.walkFileTree(dir, new SimpleFileVisitor<>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException ex) throws IOException {
				Files.delete(dir);
				return FileVisitResult.CONTINUE;
			}
		});
	}
}
