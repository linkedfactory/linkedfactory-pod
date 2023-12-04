package io.github.linkedfactory.core.cli;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.http.KvinHttp;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;
import io.github.linkedfactory.core.kvin.parquet.KvinParquet;
import io.github.linkedfactory.core.kvin.util.JsonFormatWriter;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.NoSuchElementException;

@Command(name = "CLI",
		subcommands = {CLI.Copy.class, CLI.Fetch.class, CommandLine.HelpCommand.class},
		description = "Interact with KVIN stores")
public class CLI {
	@Option(names = {"-s", "--store"}, paramLabel = "<location>", required = true, description = "location of the KVIN store")
	protected String storeLocation;

	public static void main(String[] args) {
		int exitCode = new CommandLine(new CLI()).execute(args);
		System.exit(exitCode);
	}

	private static Kvin createStore(String storeLocation) throws IOException {
		return createStore(storeLocation, null);
	}

	private static Kvin createStore(String storeLocation, String type) throws IOException {
		URI locationUri = URIs.createURI(storeLocation);
		Path path;
		if (locationUri.scheme() != null && locationUri.scheme().startsWith("http")) {
			return new KvinHttp(storeLocation);
		} else if (locationUri.isFile()) {
			path = Paths.get(locationUri.toFileString());
		} else {
			path = Paths.get(storeLocation);
		}
		if ("leveldb".equals(type) || Files.isDirectory(path.resolve("ids"))) {
			if (!Files.isDirectory(path)) {
				Files.createDirectories(path);
			}
			return new KvinLevelDb(path.toFile());
		} else if ("parquet".equals(type) || Files.isDirectory(path.resolve("metadata"))) {
			if (!Files.isDirectory(path)) {
				Files.createDirectories(path);
			}
			return new KvinParquet(path.toString());
		} else {
			throw new IllegalArgumentException("Invalid store location: " + storeLocation);
		}
	}

	@Command(description = "Retrieves all descendants of a given item")
	int descendants(@Parameters(paramLabel = "<item>", defaultValue = "", description = "the target item") String item) {
		URI itemUri = URIs.createURI(item);
		try (Kvin store = createStore(storeLocation)) {
			store.descendants(itemUri).forEach(d -> {
				System.out.println(d);
			});
		} catch (Exception e) {
			System.err.println(e.getMessage());
			return 1;
		}
		return 0;
	}

	@Command(description = "Retrieves all properties of a given item")
	int properties(@Parameters(paramLabel = "<item>", description = "the target item") String item) {
		URI itemUri = URIs.createURI(item);
		try (Kvin store = createStore(storeLocation)) {
			store.properties(itemUri).forEach(p -> {
				System.out.println(p);
			});
		} catch (Exception e) {
			System.err.println(e.getMessage());
			return 1;
		}
		return 0;
	}

	@Command(name = "copy", description = "Copies values from one store to another")
	static class Copy extends FetchBase implements Runnable {
		@Parameters(paramLabel = "<sink>", description = "location of the sink KVIN store")
		protected String sinkLocation;
		@Option(names = {"--sink-type"}, description = "type of the sink KVIN store (if newly created): leveldb, parquet")
		String sinkType;

		@Override
		public void run() {
			URI propertyUri = property == null ? null : URIs.createURI(property);
			URI contextUri = context == null ? Kvin.DEFAULT_CONTEXT : URIs.createURI(context);

			try (Kvin store = createStore(cli.storeLocation)) {
				Kvin sink;
				try {
					sink = createStore(sinkLocation, sinkType);
				} catch (IllegalArgumentException e) {
					System.err.println("Please specify --sink-type");
					throw e;
				}

				try (sink) {
					IExtendedIterator<URI> items;
					if ("*".equals(item)) {
						items = store.descendants(URIs.createURI(""));
					} else {
						items = WrappedIterator.create(Arrays.asList(URIs.createURI(item)).iterator());
					}

					IExtendedIterator<KvinTuple> allTuples = new NiceIterator<>() {
						KvinTuple next;
						IExtendedIterator<KvinTuple> tuples;

						@Override
						public boolean hasNext() {
							if (next != null) {
								return true;
							}
							while ((tuples == null || !tuples.hasNext()) && items.hasNext()) {
								if (tuples != null) {
									tuples.close();
								}
								tuples = store.fetch(items.next(), propertyUri, contextUri,
										to != null ? to : KvinTuple.TIME_MAX_VALUE,
										from != null ? from : 0, limit != null ? limit : 0, 0, null);
							}
							if (tuples != null) {
								if (tuples.hasNext()) {
									next = tuples.next();
								} else {
									tuples.close();
								}
							}
							if (next == null) {
								items.close();
								return false;
							}
							return true;
						}

						@Override
						public KvinTuple next() {
							if (!hasNext()) {
								throw new NoSuchElementException();
							}
							KvinTuple result = next;
							next = null;
							return result;
						}

						@Override
						public void close() {
							items.close();
							if (tuples != null) {
								tuples.close();
							}
							super.close();
						}
					};

					try {
						sink.put(allTuples);
					} finally {
						allTuples.close();
					}
				}
			} catch (Exception e) {
				System.err.println(e.getMessage());
				throw (e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e));
			}
		}
	}

	@Command(name = "fetch", description = "Fetches values for items")
	static class Fetch extends FetchBase implements Runnable {
		@Option(names = {"-i", "--interval"})
		Long interval;
		@Option(names = {"-o", "--op"})
		String op;

		@Override
		public void run() {
			URI propertyUri = property == null ? null : URIs.createURI(property);
			URI contextUri = context == null ? Kvin.DEFAULT_CONTEXT : URIs.createURI(context);

			try (Kvin store = createStore(cli.storeLocation)) {
				IExtendedIterator<URI> items;
				if ("*".equals(item)) {
					items = store.descendants(URIs.createURI(""));
				} else {
					items = WrappedIterator.create(Arrays.asList(URIs.createURI(item)).iterator());
				}
				try {
					for (URI itemUri : items) {
						IExtendedIterator<KvinTuple> tuples;
						if (op != null) {
							tuples = store.fetch(itemUri, propertyUri, contextUri, to != null ? to : KvinTuple.TIME_MAX_VALUE,
									from != null ? from : 0, limit != null ? limit : 0, interval != 0 ? interval : 0, op);
						} else {
							tuples = store.fetch(itemUri, propertyUri, contextUri, limit != null ? limit : 0);
						}
						try {
							JsonFormatWriter writer = new JsonFormatWriter(System.out, this.prettyPrint);
							while (tuples.hasNext()) {
								writer.writeTuple(tuples.next());
							}
							writer.close();
						} finally {
							tuples.close();
						}
					}
				} finally {
					items.close();
				}
			} catch (Exception e) {
				System.err.println(e.getMessage());
				throw (e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e));
			}
		}
	}

	static abstract class FetchBase {
		@ParentCommand
		CLI cli;

		@Parameters(paramLabel = "<item>", description = "the item or *")
		String item;
		@Option(names = {"-p", "--property"})
		String property;
		@Option(names = {"-c", "--context"})
		String context;
		@Option(names = {"-f", "--from"})
		Long from;
		@Option(names = {"-t", "--to"})
		Long to;
		@Option(names = {"-l", "--limit"})
		Long limit;
		@Option(names = {"-P", "--pretty"})
		boolean prettyPrint;
	}
}